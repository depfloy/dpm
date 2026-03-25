package process

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	dpmlog "github.com/depfloy/dpm/internal/log"
	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// Status constants for process states.
const (
	StatusOnline   = "online"
	StatusStopped  = "stopped"
	StatusStarting = "starting"
	StatusErrored  = "errored"
	StatusStopping = "stopping"
)

// Info holds runtime information about a managed process instance.
type Info struct {
	Name         string            `json:"name"`
	PID          int               `json:"pid"`
	Status       string            `json:"status"`
	Port         int               `json:"port"`
	Type         string            `json:"type"`
	Memory       uint64            `json:"memory_bytes"`
	CPU          float64           `json:"cpu_percent"`
	Uptime       time.Duration     `json:"uptime_ns"`
	RestartCount int               `json:"restart_count"`
	StartedAt    time.Time         `json:"started_at"`
	Command      string            `json:"command"`
	CWD          string            `json:"cwd"`
	Env          map[string]string `json:"env,omitempty"`
}

// managed represents a single running process instance.
type managed struct {
	config   *config.ProcessConfig
	cmd      *exec.Cmd
	pid      int
	port     int
	instance int // instance index (0, 1, ...)
	status   string
	startedAt time.Time
	restarts int
	stopCh   chan struct{}
}

// Manager handles process lifecycle operations.
type Manager struct {
	mu             sync.RWMutex
	processes      map[string]*managed // key: "name" or "name:instance"
	store          *state.Store
	logDir         string
	maxLogSize     int64
	maxLogBackups  int
	logCompress    bool
	onStatusChange func(name, status string)
}

// NewManager creates a new process manager.
func NewManager(store *state.Store, logDir string, rotation config.RotationConfig) *Manager {
	return &Manager{
		processes:     make(map[string]*managed),
		store:         store,
		logDir:        logDir,
		maxLogSize:    dpmlog.ParseMaxSize(rotation.MaxSize),
		maxLogBackups: rotation.MaxBackups,
		logCompress:   rotation.Compress,
	}
}

// OnStatusChange registers a callback for process status changes.
func (m *Manager) OnStatusChange(fn func(name, status string)) {
	m.onStatusChange = fn
}

// Start launches a new process based on the given config.
// For cluster mode, starts workers based on CPU cores.
// For legacy mode, starts based on Instances count.
func (m *Manager) Start(cfg *config.ProcessConfig, ports []int) error {
	workerCount := cfg.ResolveWorkerCount()

	for i := 0; i < workerCount; i++ {
		key := instanceKey(cfg.Name, i, workerCount)
		port := 0
		if i < len(ports) {
			port = ports[i]
		}

		if err := m.startInstance(cfg, key, i, port); err != nil {
			return fmt.Errorf("start instance %s: %w", key, err)
		}
	}

	return nil
}

// startInstance starts a single process instance.
func (m *Manager) startInstance(cfg *config.ProcessConfig, key string, instance, port int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing instance if running
	if existing, ok := m.processes[key]; ok {
		m.stopProcess(existing)
	}

	// Build environment
	env := os.Environ()
	for k, v := range cfg.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Inject port via the correct env var based on framework
	if port > 0 {
		portEnv := portEnvVar(cfg.Type, cfg.Env)
		env = append(env, fmt.Sprintf("%s=%d", portEnv, port))
	}

	// Build command - use shell to handle complex commands
	cmd := exec.Command("sh", "-c", cfg.Command)
	cmd.Dir = cfg.CWD // Symlink path, NOT resolved
	cmd.Env = env

	// Set process group so we can kill the whole tree
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Set up log files with timestamp prefix writer
	logFile, errFile, err := m.openLogFiles(cfg.Name, instance)
	if err != nil {
		return fmt.Errorf("open log files: %w", err)
	}
	cmd.Stdout = &timestampWriter{w: logFile}
	cmd.Stderr = &timestampWriter{w: errFile}

	// Start process
	if err := cmd.Start(); err != nil {
		logFile.Close()
		errFile.Close()
		return fmt.Errorf("start process: %w", err)
	}

	proc := &managed{
		config:    cfg,
		cmd:       cmd,
		pid:       cmd.Process.Pid,
		port:      port,
		instance:  instance,
		status:    StatusStarting,
		startedAt: time.Now(),
		stopCh:    make(chan struct{}),
	}

	m.processes[key] = proc

	// Persist state
	m.persistProcess(proc, key)

	// Monitor process in background
	go m.monitor(proc, key, logFile, errFile)

	return nil
}

// Stop terminates a process and all its instances.
func (m *Manager) Stop(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stopped := 0
	for key, proc := range m.processes {
		if proc.config.Name == name {
			m.stopProcess(proc)
			proc.status = StatusStopped
			m.persistProcess(proc, key)
			stopped++
		}
	}

	if stopped == 0 {
		return fmt.Errorf("process not found: %s", name)
	}
	return nil
}

// Restart stops and starts a process.
func (m *Manager) Restart(name string) error {
	m.mu.RLock()
	var cfg *config.ProcessConfig
	var ports []int
	for _, proc := range m.processes {
		if proc.config.Name == name {
			cfg = proc.config
			ports = append(ports, proc.port)
		}
	}
	m.mu.RUnlock()

	if cfg == nil {
		return fmt.Errorf("process not found: %s", name)
	}

	if err := m.Stop(name); err != nil {
		return err
	}

	// Brief pause to let port free
	time.Sleep(500 * time.Millisecond)

	return m.Start(cfg, ports)
}

// Delete stops and removes a process from management.
func (m *Manager) Delete(name string) error {
	if err := m.Stop(name); err != nil {
		// Process might already be stopped, continue with deletion
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for key, proc := range m.processes {
		if proc.config.Name == name {
			delete(m.processes, key)
			m.store.DeleteProcess(key)
		}
	}
	return nil
}

// List returns info about all managed processes.
func (m *Manager) List() []Info {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var infos []Info
	for _, proc := range m.processes {
		info := Info{
			Name:         instanceKey(proc.config.Name, proc.instance, proc.config.Instances),
			PID:          proc.pid,
			Status:       proc.status,
			Port:         proc.port,
			Type:         proc.config.Type,
			Memory:       getProcessMemory(proc.pid),
			RestartCount: proc.restarts,
			StartedAt:    proc.startedAt,
			Command:      proc.config.Command,
			CWD:          proc.config.CWD,
		}
		if proc.status == StatusOnline {
			info.Uptime = time.Since(proc.startedAt)
		}
		infos = append(infos, info)
	}
	return infos
}

// GetInfo returns detailed info about a specific process.
func (m *Manager) GetInfo(name string) ([]Info, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var infos []Info
	for _, proc := range m.processes {
		if proc.config.Name == name {
			info := Info{
				Name:         instanceKey(proc.config.Name, proc.instance, proc.config.Instances),
				PID:          proc.pid,
				Status:       proc.status,
				Port:         proc.port,
				Type:         proc.config.Type,
				Memory:       getProcessMemory(proc.pid),
				RestartCount: proc.restarts,
				StartedAt:    proc.startedAt,
				Command:      proc.config.Command,
				CWD:          proc.config.CWD,
				Env:          proc.config.Env,
			}
			if proc.status == StatusOnline {
				info.Uptime = time.Since(proc.startedAt)
			}
			infos = append(infos, info)
		}
	}

	if len(infos) == 0 {
		return nil, fmt.Errorf("process not found: %s", name)
	}
	return infos, nil
}

// GetConfig returns the process config by name.
func (m *Manager) GetConfig(name string) *config.ProcessConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, proc := range m.processes {
		if proc.config.Name == name {
			return proc.config
		}
	}
	return nil
}

// Attach re-adopts an orphan process by PID after a daemon restart.
func (m *Manager) Attach(ps *state.ProcessState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if PID is alive
	if !processAlive(ps.PID) {
		return fmt.Errorf("process %s (pid %d) is not alive", ps.Name, ps.PID)
	}

	var cfg config.ProcessConfig
	if err := json.Unmarshal(ps.ConfigJSON, &cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	proc := &managed{
		config:    &cfg,
		pid:       ps.PID,
		port:      ps.Port,
		instance:  0, // Will be parsed from key
		status:    StatusOnline,
		startedAt: ps.StartedAt,
		restarts:  ps.RestartCount,
		stopCh:    make(chan struct{}),
	}

	m.processes[ps.Name] = proc

	// Monitor the re-adopted process
	go m.monitorAdopted(proc, ps.Name)

	return nil
}

// stopProcess sends the configured stop signal and waits, then SIGKILL if needed.
func (m *Manager) stopProcess(proc *managed) {
	stopSig := resolveSignal(proc.config.StopSignal)
	stopTimeout := resolveTimeout(proc.config.StopTimeout, 10*time.Second)

	if proc.cmd == nil || proc.cmd.Process == nil {
		// Adopted process without cmd reference
		if proc.pid > 0 {
			syscall.Kill(-proc.pid, stopSig)
			time.Sleep(stopTimeout)
			if processAlive(proc.pid) {
				syscall.Kill(-proc.pid, syscall.SIGKILL)
			}
		}
		return
	}

	// Safe close - channel may already be closed from a previous stop
	select {
	case <-proc.stopCh:
		// Already closed
	default:
		close(proc.stopCh)
	}
	proc.status = StatusStopping

	// Send configured signal to process group
	pgid, err := syscall.Getpgid(proc.pid)
	if err == nil {
		syscall.Kill(-pgid, stopSig)
	}

	// Wait for graceful shutdown up to configured timeout
	done := make(chan struct{})
	go func() {
		proc.cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Process exited gracefully
	case <-time.After(stopTimeout):
		// Force kill
		if pgid > 0 {
			syscall.Kill(-pgid, syscall.SIGKILL)
		}
		proc.cmd.Wait()
	}
}

// monitor watches a started process for exit and handles restarts.
func (m *Manager) monitor(proc *managed, key string, logFile, errFile io.Closer) {
	defer logFile.Close()
	defer errFile.Close()

	// Panic recovery - never crash the daemon
	defer func() {
		if r := recover(); r != nil {
			proc.status = StatusErrored
			m.persistProcess(proc, key)
		}
	}()

	// Mark as online after brief startup period
	time.Sleep(2 * time.Second)
	if processAlive(proc.pid) {
		m.mu.Lock()
		proc.status = StatusOnline
		m.persistProcess(proc, key)
		m.mu.Unlock()
		m.notifyStatusChange(key, StatusOnline)
	}

	// Wait for exit
	proc.cmd.Wait()

	select {
	case <-proc.stopCh:
		// Intentional stop
		return
	default:
		// Unexpected exit
	}

	m.mu.Lock()

	// Check restart policy
	shouldRestart := false
	switch proc.config.RestartPolicy {
	case "always":
		shouldRestart = true
	case "on-failure":
		if proc.cmd.ProcessState != nil && !proc.cmd.ProcessState.Success() {
			shouldRestart = true
		}
	case "never":
		shouldRestart = false
	}

	// Check max restarts - default limit 50 if not configured
	maxRestarts := proc.config.MaxRestarts
	if maxRestarts <= 0 {
		maxRestarts = 50 // Safety net: never restart infinitely
	}
	if proc.restarts >= maxRestarts {
		shouldRestart = false
	}

	if !shouldRestart {
		proc.status = StatusStopped
		m.persistProcess(proc, key)
		m.mu.Unlock()
		m.notifyStatusChange(key, StatusStopped)
		return
	}

	// Restart with exponential backoff
	proc.restarts++
	delay := restartBackoff(proc.restarts)
	proc.status = StatusStarting
	m.persistProcess(proc, key)
	m.mu.Unlock()

	time.Sleep(delay)
	m.startInstance(proc.config, key, proc.instance, proc.port)
}

// monitorAdopted watches an adopted process (no cmd reference).
func (m *Manager) monitorAdopted(proc *managed, key string) {
	defer func() {
		if r := recover(); r != nil {
			proc.status = StatusErrored
			m.persistProcess(proc, key)
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !processAlive(proc.pid) {
			m.mu.Lock()

			shouldRestart := proc.config.RestartPolicy == "always"
			if proc.config.MaxRestarts > 0 && proc.restarts >= proc.config.MaxRestarts {
				shouldRestart = false
			}

			if shouldRestart {
				proc.restarts++
				m.mu.Unlock()
				m.startInstance(proc.config, key, proc.instance, proc.port)
			} else {
				proc.status = StatusStopped
				m.persistProcess(proc, key)
				m.mu.Unlock()
			}
			return
		}
	}
}

// persistProcess saves process state to BoltDB.
func (m *Manager) persistProcess(proc *managed, key string) {
	cfgJSON, _ := json.Marshal(proc.config)
	ps := &state.ProcessState{
		Name:          key,
		PID:           proc.pid,
		Port:          proc.port,
		Status:        proc.status,
		Command:       proc.config.Command,
		CWD:           proc.config.CWD,
		Type:          proc.config.Type,
		Env:           proc.config.Env,
		Instances:     proc.config.Instances,
		RestartPolicy: proc.config.RestartPolicy,
		RestartCount:  proc.restarts,
		MaxRestarts:   proc.config.MaxRestarts,
		MaxMemory:     "",
		StartedAt:     proc.startedAt,
		ConfigJSON:    cfgJSON,
	}
	if proc.config.Resources != nil {
		ps.MaxMemory = proc.config.Resources.MaxMemory
	}
	m.store.SaveProcess(ps)
}

func (m *Manager) notifyStatusChange(name, status string) {
	if m.onStatusChange != nil {
		m.onStatusChange(name, status)
	}
}

// openLogFiles creates rotating log writers for a process instance.
func (m *Manager) openLogFiles(name string, instance int) (io.WriteCloser, io.WriteCloser, error) {
	dir := fmt.Sprintf("%s/apps/%s", m.logDir, name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, err
	}

	logPath := fmt.Sprintf("%s/current.log", dir)
	errPath := fmt.Sprintf("%s/error.log", dir)

	if instance > 0 {
		logPath = fmt.Sprintf("%s/instance-%d.log", dir, instance)
		errPath = fmt.Sprintf("%s/instance-%d.error.log", dir, instance)
	}

	logWriter, err := dpmlog.NewRotatingWriter(logPath, m.maxLogSize, m.maxLogBackups, m.logCompress)
	if err != nil {
		return nil, nil, err
	}

	errWriter, err := dpmlog.NewRotatingWriter(errPath, m.maxLogSize, m.maxLogBackups, m.logCompress)
	if err != nil {
		logWriter.Close()
		return nil, nil, err
	}

	return logWriter, errWriter, nil
}

// instanceKey generates a key for a process instance.
func instanceKey(name string, instance, total int) string {
	if total <= 1 {
		return name
	}
	return fmt.Sprintf("%s:%d", name, instance)
}

// portEnvVar returns the environment variable name for port injection.
func portEnvVar(processType string, env map[string]string) string {
	// Check if user specified a custom port env var
	if v, ok := env["DPM_PORT_ENV"]; ok {
		return v
	}

	switch strings.ToLower(processType) {
	case "nodejs":
		return "PORT"
	default:
		return "PORT"
	}
}

// processAlive checks if a process with the given PID is still running.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal 0 checks existence without sending a signal
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}

// getProcessMemory returns the RSS memory usage in bytes for a PID.
func getProcessMemory(pid int) uint64 {
	if pid <= 0 {
		return 0
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	rss, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	pageSize := uint64(os.Getpagesize())
	return rss * pageSize
}

// restartBackoff calculates delay based on restart count with exponential backoff.
func restartBackoff(restarts int) time.Duration {
	switch {
	case restarts <= 1:
		return 1 * time.Second
	case restarts <= 3:
		return 2 * time.Second
	case restarts <= 5:
		return 5 * time.Second
	case restarts <= 10:
		return 10 * time.Second
	default:
		return 30 * time.Second
	}
}

// resolveSignal converts a signal name string to a syscall.Signal.
func resolveSignal(name string) syscall.Signal {
	switch strings.ToUpper(name) {
	case "SIGKILL":
		return syscall.SIGKILL
	case "SIGINT":
		return syscall.SIGINT
	case "SIGQUIT":
		return syscall.SIGQUIT
	default:
		return syscall.SIGTERM
	}
}

// resolveTimeout parses a duration string (e.g. "10s") with a fallback default.
func resolveTimeout(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}

// timestampWriter wraps an io.Writer and prepends ISO8601 timestamp to each line.
// Continuation lines (stack traces starting with "at ", whitespace, or common
// error continuation patterns) are written without a new timestamp so the log
// parser can group them with the preceding entry.
type timestampWriter struct {
	w      io.Writer
	buf    []byte
	lastTS string
}

func (tw *timestampWriter) Write(p []byte) (int, error) {
	tw.buf = append(tw.buf, p...)

	for {
		idx := -1
		for i, b := range tw.buf {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}

		line := string(tw.buf[:idx])
		tw.buf = tw.buf[idx+1:]

		if line == "" {
			continue
		}

		if isContinuationLine(line) {
			// Continuation: use same timestamp, indent with tab
			_, err := fmt.Fprintf(tw.w, "%s \t%s\n", tw.lastTS, line)
			if err != nil {
				return len(p), err
			}
		} else {
			tw.lastTS = time.Now().UTC().Format(time.RFC3339)
			_, err := fmt.Fprintf(tw.w, "%s %s\n", tw.lastTS, line)
			if err != nil {
				return len(p), err
			}
		}
	}

	return len(p), nil
}

// isContinuationLine detects stack trace and multi-line error continuation lines.
func isContinuationLine(line string) bool {
	if len(line) == 0 {
		return false
	}
	// Lines starting with whitespace, "at ", "}", "  code:", "  errno:", "  syscall:"
	if line[0] == ' ' || line[0] == '\t' {
		return true
	}
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "at ") {
		return true
	}
	if trimmed == "}" || trimmed == "{" || trimmed == "})" {
		return true
	}
	if strings.HasPrefix(trimmed, "code:") || strings.HasPrefix(trimmed, "errno:") ||
		strings.HasPrefix(trimmed, "syscall:") || strings.HasPrefix(trimmed, "address:") ||
		strings.HasPrefix(trimmed, "port:") {
		return true
	}
	return false
}
