package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/depfloy/dpm/internal/api"
	"github.com/depfloy/dpm/internal/health"
	"github.com/depfloy/dpm/internal/nginx"
	"github.com/depfloy/dpm/internal/port"
	"github.com/depfloy/dpm/internal/process"
	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// Version is set at build time via ldflags.
var Version = "dev"

// Daemon is the main DPM daemon that manages all subsystems.
type Daemon struct {
	config       *config.DaemonConfig
	store        *state.Store
	processManager *process.Manager
	portManager    *port.Manager
	healthChecker  *health.Checker
	nginxManager   *nginx.Manager
	apiServer    *http.Server
	listener     net.Listener
	logger       *slog.Logger
	stopCh       chan struct{}
}

// New creates a new daemon instance.
func New(cfg *config.DaemonConfig) (*Daemon, error) {
	// Initialize logger
	logFile, err := os.OpenFile(cfg.Daemon.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// Fall back to stderr
		logFile = os.Stderr
	}

	logger := slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Open state store
	store, err := state.Open(cfg.State.Dir)
	if err != nil {
		return nil, fmt.Errorf("open state store: %w", err)
	}

	// Initialize subsystems
	pm := process.NewManager(store, cfg.Logging.Dir, cfg.Logging.Rotation)
	portMgr := port.NewManager(store, cfg.Ports)
	hc := health.NewChecker()
	nginxMgr := nginx.NewManager(cfg.Nginx.ConfigDir, cfg.Nginx.ReloadCommand, pm)

	// Wire up health check → nginx upstream integration
	hc.OnUnhealthy(func(name string, status *health.Status) {
		logger.Warn("process unhealthy - removing from upstream",
			"name", name,
			"message", status.Message,
			"consecutive_failures", status.Consecutive,
		)
		// Find process info to get port and domain
		infos, err := pm.GetInfo(name)
		if err == nil && len(infos) > 0 && infos[0].Port > 0 {
			// Find nginx config for this process (sites-enabled files)
			entries, _ := os.ReadDir(filepath.Join(cfg.Nginx.ConfigDir, "sites-enabled"))
			for _, e := range entries {
				configPath := filepath.Join(cfg.Nginx.ConfigDir, "sites-available", e.Name())
				data, _ := os.ReadFile(configPath)
				if strings.Contains(string(data), fmt.Sprintf("upstream_%s", strings.Split(name, ":")[0])) {
					if err := nginxMgr.MarkWorkerDown(e.Name(), infos[0].Port); err != nil {
						logger.Error("failed to remove unhealthy worker from upstream", "error", err)
					} else {
						logger.Info("removed unhealthy worker from upstream", "name", name, "port", infos[0].Port)
					}
					break
				}
			}
		}
	})

	hc.OnHealthy(func(name string, status *health.Status) {
		logger.Info("process recovered - adding back to upstream",
			"name", name,
		)
		infos, err := pm.GetInfo(name)
		if err == nil && len(infos) > 0 && infos[0].Port > 0 {
			entries, _ := os.ReadDir(filepath.Join(cfg.Nginx.ConfigDir, "sites-enabled"))
			for _, e := range entries {
				configPath := filepath.Join(cfg.Nginx.ConfigDir, "sites-available", e.Name())
				data, _ := os.ReadFile(configPath)
				if strings.Contains(string(data), fmt.Sprintf("upstream_%s", strings.Split(name, ":")[0])) {
					if err := nginxMgr.MarkWorkerUp(e.Name(), infos[0].Port); err != nil {
						logger.Error("failed to add recovered worker to upstream", "error", err)
					} else {
						logger.Info("added recovered worker to upstream", "name", name, "port", infos[0].Port)
					}
					break
				}
			}
		}
	})

	pm.OnStatusChange(func(name, status string) {
		logger.Info("process status changed", "name", name, "status", status)
	})

	d := &Daemon{
		config:         cfg,
		store:          store,
		processManager: pm,
		portManager:    portMgr,
		healthChecker:  hc,
		nginxManager:   nginxMgr,
		logger:         logger,
		stopCh:         make(chan struct{}),
	}

	return d, nil
}

// Run starts the daemon and blocks until shutdown.
func (d *Daemon) Run() error {
	d.logger.Info("DPM daemon starting", "version", Version)

	// Write PID file
	if err := d.writePIDFile(); err != nil {
		return fmt.Errorf("write pid file: %w", err)
	}
	defer os.Remove(d.config.Daemon.PIDFile)

	// Adopt orphan processes from previous daemon instance
	if err := d.adoptOrphans(); err != nil {
		d.logger.Warn("orphan adoption had errors", "error", err)
	}

	// Start API server on Unix socket
	if err := d.startAPI(); err != nil {
		return fmt.Errorf("start api: %w", err)
	}

	d.logger.Info("DPM daemon ready",
		"socket", d.config.Daemon.Socket,
		"version", Version,
	)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	for {
		select {
		case sig := <-sigCh:
			switch sig {
			case syscall.SIGHUP:
				d.logger.Info("received SIGHUP, reloading config")
				// TODO: Reload config
				continue
			case syscall.SIGTERM, syscall.SIGINT:
				d.logger.Info("received shutdown signal", "signal", sig.String())
				return d.shutdown()
			}
		case <-d.stopCh:
			return d.shutdown()
		}
	}
}

// adoptOrphans re-attaches processes that survived a daemon restart.
func (d *Daemon) adoptOrphans() error {
	processes, err := d.store.ListProcesses()
	if err != nil {
		return fmt.Errorf("list processes: %w", err)
	}

	if len(processes) == 0 {
		d.logger.Info("no orphan processes to adopt")
		return nil
	}

	adopted := 0
	restarted := 0
	cleaned := 0

	for _, ps := range processes {
		// Clean up processes that exceeded max restarts or are in error state
		if ps.RestartCount >= 50 || ps.Status == "errored" {
			d.store.DeleteProcess(ps.Name)
			cleaned++
			d.logger.Info("cleaned stale process from state",
				"name", ps.Name,
				"restarts", ps.RestartCount,
				"status", ps.Status,
			)
			continue
		}

		if ps.PID <= 0 {
			// No PID - remove from state
			d.store.DeleteProcess(ps.Name)
			cleaned++
			continue
		}

		if processAlive(ps.PID) {
			if err := d.processManager.Attach(ps); err != nil {
				d.logger.Warn("failed to adopt process",
					"name", ps.Name,
					"pid", ps.PID,
					"error", err,
				)
				d.store.DeleteProcess(ps.Name)
				cleaned++
				continue
			}
			adopted++
			d.logger.Info("re-adopted process",
				"name", ps.Name,
				"pid", ps.PID,
			)
		} else {
			// Process is dead - remove from state, don't auto-restart
			// Restart will happen on next deploy or manual dpm start
			d.store.DeleteProcess(ps.Name)
			cleaned++
			d.logger.Info("removed dead process from state",
				"name", ps.Name,
				"pid", ps.PID,
			)
		}
	}

	d.logger.Info("orphan adoption complete",
		"adopted", adopted,
		"restarted", restarted,
		"cleaned", cleaned,
		"total", len(processes),
	)

	return nil
}

// startAPI starts the HTTP API server on the Unix socket.
func (d *Daemon) startAPI() error {
	// Remove stale socket file
	os.Remove(d.config.Daemon.Socket)

	// Ensure socket directory exists
	socketDir := d.config.Daemon.Socket[:len(d.config.Daemon.Socket)-len("/dpm.sock")]
	if err := os.MkdirAll(socketDir, 0755); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}

	listener, err := net.Listen("unix", d.config.Daemon.Socket)
	if err != nil {
		return fmt.Errorf("listen unix socket: %w", err)
	}

	// Set socket permissions so configured user can access
	os.Chmod(d.config.Daemon.Socket, 0666)

	handler := api.NewRouter(
		d.processManager,
		d.portManager,
		d.healthChecker,
		d.nginxManager,
		d.store,
		d.config,
		d.logger,
	)

	d.listener = listener
	d.apiServer = &http.Server{Handler: handler}

	go func() {
		if err := d.apiServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			d.logger.Error("api server error", "error", err)
		}
	}()

	return nil
}

// shutdown performs graceful daemon shutdown.
func (d *Daemon) shutdown() error {
	d.logger.Info("shutting down daemon")

	// Note: We do NOT stop managed processes here.
	// KillMode=process in systemd ensures child processes survive.
	// We only save state so the next daemon instance can adopt them.

	// Close API server
	if d.apiServer != nil {
		d.apiServer.Close()
	}
	if d.listener != nil {
		d.listener.Close()
	}

	// Persist final state - save all process info so next daemon can adopt them
	d.logger.Info("saving final state")
	processes := d.processManager.List()
	for _, p := range processes {
		if p.PID > 0 {
			d.logger.Info("saving process state for adoption",
				"name", p.Name,
				"pid", p.PID,
				"port", p.Port,
				"status", p.Status,
			)
		}
	}

	// Close state store
	if err := d.store.Close(); err != nil {
		d.logger.Error("failed to close state store", "error", err)
	}

	d.logger.Info("daemon stopped")
	return nil
}

// writePIDFile writes the current process PID to the configured file.
func (d *Daemon) writePIDFile() error {
	pidDir := d.config.Daemon.PIDFile[:len(d.config.Daemon.PIDFile)-len("/dpm.pid")]
	if err := os.MkdirAll(pidDir, 0755); err != nil {
		return err
	}
	return os.WriteFile(d.config.Daemon.PIDFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
}

// processAlive checks if a process with the given PID exists.
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
