package daemon

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	portMgr := port.NewManager(store)
	hc := health.NewChecker()
	nginxMgr := nginx.NewManager(cfg.Nginx.ConfigDir, cfg.Nginx.ReloadCommand, pm)

	// Health check callbacks - log only, nginx is managed by Depfloy
	hc.OnUnhealthy(func(name string, status *health.Status) {
		logger.Warn("process unhealthy",
			"name", name,
			"message", status.Message,
			"consecutive_failures", status.Consecutive,
		)
	})

	hc.OnHealthy(func(name string, status *health.Status) {
		logger.Info("process recovered", "name", name)
	})

	pm.OnStatusChange(func(name, status string) {
		logger.Info("process status changed", "name", name, "status", status)
	})

	pm.OnMemoryLimit(func(name string, rss, limit uint64) {
		logger.Warn("memory limit exceeded, replacing",
			"name", name,
			"rss_bytes", rss,
			"limit_bytes", limit,
		)
	})

	pm.OnRecycle(func(name, event, detail string) {
		// "aborted" is the one worth waking up for: a replacement that would not
		// come up means the application is still serving on a process that is
		// over its ceiling, and it will keep trying.
		if event == "aborted" || event == "unavailable" {
			logger.Warn("recycle handover "+event, "name", name, "detail", detail)
			return
		}
		logger.Info("recycle handover "+event, "name", name, "detail", detail)
	})

	// The zero-downtime handover needs the shim present: a replacement can only
	// bind a port its predecessor still holds when both sockets set SO_REUSEPORT.
	// Missing file means fall back to the in-place restart rather than fail to
	// start, so a partial install degrades instead of taking the box down.
	if shim := cfg.Recycle.ReusePortShim; shim != "" {
		if _, err := os.Stat(shim); err != nil {
			logger.Warn("reuseport shim not found; memory recycles will restart in place",
				"path", shim, "error", err)
		} else {
			pm.SetReusePortShim(shim)
			logger.Info("zero-downtime memory recycle enabled", "shim", shim)
		}
	} else {
		logger.Info("zero-downtime memory recycle disabled by config")
	}

	if d, err := time.ParseDuration(cfg.Recycle.HealthTimeout); err == nil && d > 0 {
		pm.SetRecycleHealthTimeout(d)
	}
	if d, err := time.ParseDuration(cfg.Recycle.Settle); err == nil && d > 0 {
		pm.SetRecycleSettle(d)
	}

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
// Dead processes are automatically restarted from their saved config.
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

	// Collect dead processes for restart (deduplicated by base name).
	// A multi-worker process has several instance keys ("app:0", "app:1", ...)
	// each with its own port; we must collect ALL of them so the restart recreates
	// the correct worker count (process.Manager.Start derives it from len(ports)).
	type restartInfo struct {
		cfg   *config.ProcessConfig
		ports []int
	}
	toRestart := make(map[string]*restartInfo) // key: base process name

	// Base names that already have a live process attached. A drifted server
	// carries two records for one application — the live one under its deploy
	// key, a dead one under the real key, both naming the same port. Restarting
	// the dead record there would put a second process on a port that is already
	// being served, so a base name adopted alive is never also restarted.
	adoptedNames := make(map[string]bool)
	for _, ps := range processes {
		if ps.PID > 0 && processAlive(ps.PID) {
			adoptedNames[baseProcessName(ps.Name)] = true
		}
	}

	for _, ps := range processes {
		// Clean up processes that exceeded max restarts or are in error state
		if ps.RestartCount >= 50 || ps.Status == "errored" || ps.Status == "stopped" {
			d.store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				d.portManager.ReleasePort(ps.Port)
			}
			cleaned++
			d.logger.Info("cleaned stale process from state",
				"name", ps.Name,
				"restarts", ps.RestartCount,
				"status", ps.Status,
			)
			continue
		}

		if ps.PID <= 0 {
			d.store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				d.portManager.ReleasePort(ps.Port)
			}
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
				if ps.Port > 0 {
					d.portManager.ReleasePort(ps.Port)
				}
				cleaned++
				continue
			}
			adopted++
			d.logger.Info("re-adopted process",
				"name", ps.Name,
				"pid", ps.PID,
			)
		} else {
			// Process is dead - collect for restart from saved config
			d.store.DeleteProcess(ps.Name)

			// Extract base name from instance key (e.g., "app_243:0" → "app_243")
			baseName := baseProcessName(ps.Name)

			// Something for this application is already running and has just been
			// adopted; this record is its stranded twin, naming that live
			// process's port. Drop the record but leave the port alone — it is in
			// use, and releasing it would offer a served port to the next
			// allocation.
			if adoptedNames[baseName] {
				d.logger.Info("dropped stale record for a process that is already running",
					"name", ps.Name,
					"port", ps.Port,
				)
				continue
			}

			if ps.Port > 0 {
				d.portManager.ReleasePort(ps.Port)
			}

			// Collect config once per base name, but accumulate every instance's port.
			if existing, exists := toRestart[baseName]; exists {
				if ps.Port > 0 {
					existing.ports = append(existing.ports, ps.Port)
				}
			} else {
				var cfg config.ProcessConfig
				if err := json.Unmarshal(ps.ConfigJSON, &cfg); err == nil && cfg.Name != "" {
					var ports []int
					if ps.Port > 0 {
						ports = append(ports, ps.Port)
					}
					toRestart[baseName] = &restartInfo{cfg: &cfg, ports: ports}
					d.logger.Info("will restart dead process",
						"name", baseName,
						"port", ps.Port,
					)
				} else {
					cleaned++
				}
			}
		}
	}

	// Restart dead processes from saved configs
	for name, r := range toRestart {
		if err := d.processManager.Start(r.cfg, r.ports); err != nil {
			d.logger.Error("failed to restart dead process",
				"name", name,
				"error", err,
			)
			cleaned++
		} else {
			restarted++
			d.logger.Info("restarted dead process",
				"name", name,
				"ports", r.ports,
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
		Version,
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

// baseProcessName strips the instance/deploy suffix from a state key:
// "app_243:0" and "app_243:deploy:0" both belong to "app_243".
func baseProcessName(name string) string {
	if idx := strings.Index(name, ":"); idx > 0 {
		return name[:idx]
	}
	return name
}
