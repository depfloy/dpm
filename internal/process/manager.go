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
	pendingDrain   map[string][]*managed // old workers awaiting explicit drain
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
		processes:    make(map[string]*managed),
		pendingDrain: make(map[string][]*managed),
		store:        store,
		logDir:       logDir,
		maxLogSize:   dpmlog.ParseMaxSize(rotation.MaxSize),
		maxLogBackups: rotation.MaxBackups,
		logCompress:  rotation.Compress,
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
	// Worker count is determined by explicit ports array length
	workerCount := len(ports)
	if workerCount == 0 {
		workerCount = cfg.ResolveWorkerCount()
	}

	// Stop ALL existing instances for this process name before starting new ones.
	// This handles the case where worker count changed (e.g., cluster→single).
	// Old keys like "app_238:0", "app_238:1" won't match new key "app_238".
	// Collect first, then stop WITHOUT holding the mutex to avoid blocking the daemon.
	type toStop struct {
		key  string
		proc *managed
	}
	var stopping []toStop
	m.mu.Lock()
	for key, proc := range m.processes {
		if proc.config.Name == cfg.Name {
			stopping = append(stopping, toStop{key, proc})
			delete(m.processes, key)
			m.store.DeleteProcess(key)
		}
	}
	m.mu.Unlock()

	for _, s := range stopping {
		m.stopProcess(s.proc)
	}

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

	// Stop existing instance if running, carry forward restart count
	previousRestarts := 0
	if existing, ok := m.processes[key]; ok {
		if existing.status == StatusStopped || existing.status == StatusErrored {
			// Dead process - clean start, reset counter, remove stale entry
			previousRestarts = 0
			m.store.DeleteProcess(key)
		} else {
			previousRestarts = existing.restarts
		}
		// existing is normally already dead here (monitor restart path), so the
		// kill/wait returns immediately; signalStop closes its stopCh first.
		m.signalStop(existing)
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

	// Cap how long cmd.Wait blocks on the stdout/stderr copy goroutines after the
	// process exits. A grandchild (e.g. node/python under `sh -c`) can inherit the
	// output pipe and keep it open forever, which would otherwise hang Wait and -
	// when that Wait runs under the Manager mutex - deadlock the whole daemon.
	// After WaitDelay, Wait force-closes the pipe FDs so Wait always returns.
	cmd.WaitDelay = resolveTimeout(cfg.WaitDelay, 10*time.Second)

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
		restarts:  previousRestarts,
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
	// Phase 1 (under lock): signal stop intent so monitors won't restart, mark
	// stopped, persist, and collect the procs to kill. We do NOT block here.
	m.mu.Lock()
	var toStop []*managed
	for key, proc := range m.processes {
		if proc.config.Name == name {
			m.signalStop(proc)
			proc.status = StatusStopped
			m.persistProcess(proc, key)
			toStop = append(toStop, proc)
		}
	}
	m.mu.Unlock()

	if len(toStop) == 0 {
		return fmt.Errorf("process not found: %s", name)
	}

	// Phase 2 (no lock): perform the blocking kill/wait outside the lock so other
	// manager operations are not stalled for up to stop_timeout.
	for _, proc := range toStop {
		m.stopProcess(proc)
	}
	return nil
}

// DeployResult represents the outcome of a blue-green deploy.
type DeployResult struct {
	Status   string `json:"status"`
	NewPorts []int  `json:"new_ports"`
	OldPorts []int  `json:"old_ports"`
	Workers  int    `json:"workers"`
	Message  string `json:"message,omitempty"`
}

// Deploy performs a blue-green deployment: starts new workers on new ports,
// waits for them to be online, then gracefully shuts down old workers.
// Old workers continue serving traffic until new workers are confirmed healthy,
// ensuring zero-downtime.
func (m *Manager) Deploy(cfg *config.ProcessConfig, newPorts []int) (*DeployResult, error) {
	workerCount := cfg.ResolveWorkerCount()

	// 1. Collect old worker info
	m.mu.RLock()
	var oldKeys []string
	var oldPorts []int
	for key, proc := range m.processes {
		if proc.config.Name == cfg.Name {
			oldKeys = append(oldKeys, key)
			oldPorts = append(oldPorts, proc.port)
		}
	}
	m.mu.RUnlock()

	// 2. Start new workers with deploy prefix keys (old workers still running)
	for i := 0; i < workerCount; i++ {
		deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
		port := 0
		if i < len(newPorts) {
			port = newPorts[i]
		}
		if err := m.startInstance(cfg, deployKey, i, port); err != nil {
			// Cleanup: stop any new workers already started
			for j := 0; j < i; j++ {
				cleanKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, j)
				m.mu.Lock()
				if p, ok := m.processes[cleanKey]; ok {
					m.signalStop(p)
					delete(m.processes, cleanKey)
					m.store.DeleteProcess(cleanKey)
					m.mu.Unlock()
					m.stopProcess(p)
				} else {
					m.mu.Unlock()
				}
			}
			return nil, fmt.Errorf("start new worker %d: %w", i, err)
		}
	}

	// 3. Wait for all new workers to be online (max 30s)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allOnline := true
		m.mu.RLock()
		for i := 0; i < workerCount; i++ {
			deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
			if proc, ok := m.processes[deployKey]; ok {
				if proc.status != StatusOnline {
					allOnline = false
					break
				}
			} else {
				allOnline = false
				break
			}
		}
		m.mu.RUnlock()

		if allOnline {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify all new workers are online
	m.mu.RLock()
	allOnline := true
	for i := 0; i < workerCount; i++ {
		deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
		if proc, ok := m.processes[deployKey]; ok {
			if proc.status != StatusOnline {
				allOnline = false
			}
		} else {
			allOnline = false
		}
	}
	m.mu.RUnlock()

	if !allOnline {
		// Rollback: stop new workers, keep old ones running
		for i := 0; i < workerCount; i++ {
			deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
			m.mu.Lock()
			if p, ok := m.processes[deployKey]; ok {
				m.signalStop(p)
				delete(m.processes, deployKey)
				m.store.DeleteProcess(deployKey)
				m.mu.Unlock()
				m.stopProcess(p)
			} else {
				m.mu.Unlock()
			}
		}
		return nil, fmt.Errorf("new workers failed to come online within 30s")
	}

	// 4. Promote: swap deploy keys to final keys
	m.mu.Lock()
	// Collect old workers for deferred cleanup
	type oldWorker struct {
		proc *managed
		key  string
	}
	var oldWorkers []oldWorker
	for _, oldKey := range oldKeys {
		if proc, ok := m.processes[oldKey]; ok {
			oldWorkers = append(oldWorkers, oldWorker{proc: proc, key: oldKey})
			delete(m.processes, oldKey)
		}
	}

	// Move new workers from deploy keys to final keys
	for i := 0; i < workerCount; i++ {
		deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
		finalKey := instanceKey(cfg.Name, i, workerCount)
		if proc, ok := m.processes[deployKey]; ok {
			delete(m.processes, deployKey)
			m.store.DeleteProcess(deployKey)
			m.processes[finalKey] = proc
			m.persistProcess(proc, finalKey)
		}
	}
	m.mu.Unlock()

	// 5. Store old workers for explicit drain by caller (Depfloy)
	// Old workers keep running until Drain() is called after nginx switch
	if len(oldWorkers) > 0 {
		m.mu.Lock()
		for _, ow := range oldWorkers {
			m.pendingDrain[cfg.Name] = append(m.pendingDrain[cfg.Name], ow.proc)
			m.store.DeleteProcess(ow.key)
		}
		m.mu.Unlock()
	}

	return &DeployResult{
		Status:   "success",
		NewPorts: newPorts,
		OldPorts: oldPorts,
		Workers:  workerCount,
	}, nil
}

// Drain stops old workers that were parked during a blue-green deploy.
// Called by Depfloy after nginx has been switched to the new port.
func (m *Manager) Drain(name string) error {
	m.mu.Lock()
	workers, ok := m.pendingDrain[name]
	if ok {
		delete(m.pendingDrain, name)
	}
	// Signal stop intent under the lock so the mutation of proc.status/stopCh does
	// not race with the parked workers' still-running monitor goroutines.
	for _, proc := range workers {
		m.signalStop(proc)
	}
	m.mu.Unlock()

	if !ok || len(workers) == 0 {
		return nil
	}

	// Blocking kill/wait happens outside the lock.
	for _, proc := range workers {
		m.stopProcess(proc)
	}
	return nil
}

// ReloadAll stops all processes and restarts them from saved configs.
// This is the "emergency reset" - kills everything and starts fresh.
// Returns (restarted count, failed count, error).
func (m *Manager) ReloadAll() (int, int, error) {
	// 1. Collect all unique process configs and their ports
	type savedProcess struct {
		cfg   *config.ProcessConfig
		ports []int
	}
	saved := make(map[string]*savedProcess)

	m.mu.RLock()
	for _, proc := range m.processes {
		name := proc.config.Name
		if _, ok := saved[name]; !ok {
			saved[name] = &savedProcess{cfg: proc.config}
		}
		saved[name].ports = append(saved[name].ports, proc.port)
	}
	m.mu.RUnlock()

	// Also check BoltDB for any processes not in memory
	states, _ := m.store.ListProcesses()
	for _, ps := range states {
		baseName := ps.Name
		if idx := strings.Index(ps.Name, ":"); idx > 0 {
			baseName = ps.Name[:idx]
		}
		if _, ok := saved[baseName]; !ok {
			var cfg config.ProcessConfig
			if err := json.Unmarshal(ps.ConfigJSON, &cfg); err == nil && cfg.Name != "" {
				saved[baseName] = &savedProcess{cfg: &cfg, ports: []int{ps.Port}}
			}
		}
	}

	if len(saved) == 0 {
		return 0, 0, fmt.Errorf("no processes to reload")
	}

	// 2. Collect process list and PIDs, then clear map WITHOUT blocking on stop
	m.mu.Lock()
	var pidsToKill []int
	for key, proc := range m.processes {
		// Close stopCh so monitor goroutines exit cleanly
		select {
		case <-proc.stopCh:
		default:
			close(proc.stopCh)
		}
		if proc.pid > 0 {
			pidsToKill = append(pidsToKill, proc.pid)
		}
		delete(m.processes, key)
	}
	m.mu.Unlock()

	// 3. Kill processes WITHOUT holding the mutex (non-blocking)
	for _, pid := range pidsToKill {
		syscall.Kill(-pid, syscall.SIGTERM)
	}
	time.Sleep(2 * time.Second)
	for _, pid := range pidsToKill {
		if processAlive(pid) {
			syscall.Kill(-pid, syscall.SIGKILL)
		}
	}

	// 4. Clear all process state from BoltDB
	for _, ps := range states {
		m.store.DeleteProcess(ps.Name)
	}

	time.Sleep(1 * time.Second)

	// 5. Restart each process from saved config
	restarted := 0
	failed := 0
	for _, sp := range saved {
		if err := m.Start(sp.cfg, sp.ports); err != nil {
			failed++
			continue
		}
		restarted++
	}

	return restarted, failed, nil
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

// signalStop marks a process as stopping and closes its stop channel so its
// monitor goroutine will not restart it. The caller MUST hold m.mu — it mutates
// shared proc state (status) and the stopCh. This is the "signal" half; the
// blocking kill/wait is done by stopProcess WITHOUT the lock held.
func (m *Manager) signalStop(proc *managed) {
	// Safe close - channel may already be closed from a previous stop
	select {
	case <-proc.stopCh:
	default:
		close(proc.stopCh)
	}
	proc.status = StatusStopping
}

// stopProcess sends the configured stop signal and waits, then SIGKILL if needed.
// After stopping, verifies the port is actually freed to prevent orphan issues.
//
// This is the blocking "wait" half and should be called WITHOUT m.mu held — it can
// block for up to stop_timeout, so holding the lock here would stall every other
// manager operation. It only reads immutable proc fields (cmd, pid, port), so it is
// safe to run lock-free. Callers must invoke signalStop (under the lock) first so the
// monitor goroutine sees the stop intent before the process dies.
func (m *Manager) stopProcess(proc *managed) {
	stopSig := resolveSignal(proc.config.StopSignal)
	stopTimeout := resolveTimeout(proc.config.StopTimeout, 10*time.Second)

	if proc.cmd == nil || proc.cmd.Process == nil {
		// Adopted process without cmd reference.
		// Try both process group and direct PID since adopted processes
		// may have a different PGID than their PID.
		if proc.pid > 0 {
			syscall.Kill(-proc.pid, stopSig)
			syscall.Kill(proc.pid, stopSig)
			// Poll for exit instead of sleeping a fixed duration
			for i := 0; i < 10; i++ {
				if !processAlive(proc.pid) {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if processAlive(proc.pid) {
				syscall.Kill(-proc.pid, syscall.SIGKILL)
				syscall.Kill(proc.pid, syscall.SIGKILL)
				time.Sleep(200 * time.Millisecond)
			}
		}
	} else {
		// Send configured signal to process group
		pgid, err := syscall.Getpgid(proc.pid)
		if err == nil {
			syscall.Kill(-pgid, stopSig)
		}

		// Wait for graceful shutdown up to configured timeout. cmd.Wait runs in a
		// goroutine; cmd.WaitDelay (set in startInstance) guarantees it eventually
		// returns even if a grandchild keeps the output pipe open.
		done := make(chan struct{})
		go func() {
			proc.cmd.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Process exited gracefully
		case <-time.After(stopTimeout):
			// Force kill the whole process group, then wait for the Wait goroutine
			// to unwind. WaitDelay bounds it; the extra cap is a hard safety net so
			// stopProcess can never block forever. Do NOT call cmd.Wait again here -
			// the goroutine above already owns the single Wait call.
			if pgid > 0 {
				syscall.Kill(-pgid, syscall.SIGKILL)
			}
			waitDelay := resolveTimeout(proc.config.WaitDelay, 10*time.Second)
			select {
			case <-done:
			case <-time.After(waitDelay + 2*time.Second):
			}
		}
	}

	// Last resort: if port is still held by an orphan, kill it
	if proc.port > 0 {
		time.Sleep(100 * time.Millisecond)
		killPortHolder(proc.port)
	}
}

// killPortHolder finds and kills the process listening on a port.
func killPortHolder(port int) {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return
	}
	hexPort := fmt.Sprintf("%04X", port)
	for _, line := range strings.Split(string(data), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		parts := strings.Split(fields[1], ":")
		if len(parts) == 2 && parts[1] == hexPort {
			inode := fields[9]
			pid := findPIDByInode(inode)
			if pid > 0 {
				syscall.Kill(pid, syscall.SIGKILL)
			}
			return
		}
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

	// Fast crash detection: if process lived less than 10s, lower tolerance
	uptime := time.Since(proc.startedAt)
	fastCrashLimit := 5
	if uptime < 10*time.Second && proc.restarts >= fastCrashLimit {
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

	// Re-check stop intent: the user may have called Stop during the backoff
	// window, which closes stopCh. Without this, the process would be resurrected
	// despite an explicit stop.
	select {
	case <-proc.stopCh:
		return
	default:
	}

	if err := m.startInstance(proc.config, key, proc.instance, proc.port); err != nil {
		m.mu.Lock()
		proc.status = StatusErrored
		m.persistProcess(proc, key)
		m.mu.Unlock()
		m.notifyStatusChange(key, StatusErrored)
	}
}

// monitorAdopted watches an adopted process (no cmd reference).
// If the process dies, it restarts from saved config.
func (m *Manager) monitorAdopted(proc *managed, key string) {
	defer func() {
		if r := recover(); r != nil {
			proc.status = StatusErrored
			m.persistProcess(proc, key)
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-proc.stopCh:
			// Intentional stop - do NOT restart
			return
		case <-ticker.C:
			if !processAlive(proc.pid) {
				// Check if intentionally stopped
				select {
				case <-proc.stopCh:
					return
				default:
				}

				// Process died unexpectedly - restart from saved config
				m.mu.Lock()
				delete(m.processes, key)
				m.mu.Unlock()

				if proc.config != nil && proc.config.Name != "" {
					if err := m.startInstance(proc.config, key, proc.instance, proc.port); err != nil {
						// Restart failed - keep the entry visible as errored instead
						// of silently dropping it from management entirely.
						m.mu.Lock()
						proc.status = StatusErrored
						m.processes[key] = proc
						m.persistProcess(proc, key)
						m.mu.Unlock()
						m.notifyStatusChange(key, StatusErrored)
					}
				}
				return
			}
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
// It uses brace/bracket depth tracking to group multi-line output (e.g. pretty-printed
// JSON from Node.js console.log) into a single log entry. Continuation lines are
// written with a tab marker so the log parser can merge them on read.
type timestampWriter struct {
	w         io.Writer
	buf       []byte
	lastTS    string
	depth     int       // brace/bracket nesting depth for multi-line grouping
	contCount int       // consecutive continuation lines (safety limit)
	lastWrite time.Time // last write time for depth timeout
}

// maxContinuationLines prevents runaway grouping from unmatched braces.
const maxContinuationLines = 200

func (tw *timestampWriter) Write(p []byte) (int, error) {
	// Reset depth if too much time passed — the multi-line block likely ended
	if tw.depth > 0 && !tw.lastWrite.IsZero() && time.Since(tw.lastWrite) > 2*time.Second {
		tw.depth = 0
		tw.contCount = 0
	}
	tw.lastWrite = time.Now()

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

		isCont := tw.depth > 0 || isContinuationLine(line)

		// Safety: prevent runaway continuation from unmatched braces
		if isCont {
			tw.contCount++
			if tw.contCount > maxContinuationLines {
				tw.depth = 0
				tw.contCount = 0
				isCont = false
			}
		} else {
			tw.contCount = 0
		}

		// Update brace/bracket depth AFTER deciding continuation status
		tw.depth += countDepthChange(line)
		if tw.depth < 0 {
			tw.depth = 0
		}

		if isCont {
			// Continuation: use same timestamp, tab marker for parser
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

// countDepthChange counts brace/bracket nesting changes in a line,
// properly skipping characters inside quoted strings.
func countDepthChange(line string) int {
	delta := 0
	inString := false
	escaped := false
	for _, ch := range line {
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && inString {
			escaped = true
			continue
		}
		if ch == '"' || ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '{', '[':
			delta++
		case '}', ']':
			delta--
		}
	}
	return delta
}

// DoctorReport contains the results of a health check.
type DoctorReport struct {
	Zombies       []DoctorEntry `json:"zombies"`
	Orphans       []DoctorEntry `json:"orphans"`
	ZombiesFixed  int           `json:"zombies_fixed"`
	OrphansFixed  int           `json:"orphans_fixed"`
}

// DoctorEntry represents a single issue found by Doctor.
type DoctorEntry struct {
	PID    int    `json:"pid"`
	Port   int    `json:"port"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

// Doctor performs a health check on managed processes.
// If fix is true, it cleans up zombie entries and kills orphan processes.
func (m *Manager) Doctor(fix bool) *DoctorReport {
	report := &DoctorReport{}

	// 1. Find zombie processes (stopped/errored in m.processes)
	m.mu.Lock()
	var zombieKeys []string
	for key, proc := range m.processes {
		if proc.status == StatusStopped || proc.status == StatusErrored {
			report.Zombies = append(report.Zombies, DoctorEntry{
				PID:    proc.pid,
				Port:   proc.port,
				Name:   key,
				Status: proc.status,
			})
			if fix {
				zombieKeys = append(zombieKeys, key)
			}
		}
	}
	if fix {
		for _, key := range zombieKeys {
			delete(m.processes, key)
			m.store.DeleteProcess(key)
			report.ZombiesFixed++
		}
	}
	m.mu.Unlock()

	// 2. Find orphan processes (listening on DPM port range but not managed)
	// Collect all PIDs managed by DPM
	m.mu.RLock()
	managedPIDs := make(map[int]bool)
	for _, proc := range m.processes {
		if proc.pid > 0 {
			managedPIDs[proc.pid] = true
		}
	}
	m.mu.RUnlock()

	// Parse /proc/net/tcp to find listening ports in DPM range (3000-6999)
	orphans := findOrphanListeners(managedPIDs, 3000, 6999)
	report.Orphans = orphans

	if fix {
		for _, orphan := range orphans {
			if orphan.PID > 0 {
				syscall.Kill(orphan.PID, syscall.SIGTERM)
				report.OrphansFixed++
			}
		}
		// Brief wait, then SIGKILL survivors
		if report.OrphansFixed > 0 {
			time.Sleep(2 * time.Second)
			for _, orphan := range orphans {
				if orphan.PID > 0 && processAlive(orphan.PID) {
					syscall.Kill(orphan.PID, syscall.SIGKILL)
				}
			}
		}
	}

	return report
}

// findOrphanListeners reads /proc/net/tcp to find processes listening
// on ports in the given range that are NOT in the managedPIDs set.
func findOrphanListeners(managedPIDs map[int]bool, portMin, portMax int) []DoctorEntry {
	var orphans []DoctorEntry

	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return orphans
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines[1:] { // skip header
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}

		// State 0A = LISTEN
		if fields[3] != "0A" {
			continue
		}

		// Parse local address (hex port)
		addrParts := strings.Split(fields[1], ":")
		if len(addrParts) != 2 {
			continue
		}
		port64, err := strconv.ParseInt(addrParts[1], 16, 32)
		if err != nil {
			continue
		}
		port := int(port64)

		if port < portMin || port > portMax {
			continue
		}

		// Parse inode to find PID
		inode := fields[9]
		pid := findPIDByInode(inode)
		if pid <= 0 {
			continue
		}

		if !managedPIDs[pid] {
			orphans = append(orphans, DoctorEntry{
				PID:  pid,
				Port: port,
			})
		}
	}

	return orphans
}

// findPIDByInode searches /proc/*/fd/ for a socket with the given inode.
func findPIDByInode(inode string) int {
	target := "socket:[" + inode + "]"

	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}

	for _, d := range procDirs {
		if !d.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(d.Name())
		if err != nil {
			continue
		}

		fdDir := fmt.Sprintf("/proc/%d/fd", pid)
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}

		for _, fd := range fds {
			link, err := os.Readlink(fmt.Sprintf("%s/%s", fdDir, fd.Name()))
			if err != nil {
				continue
			}
			if link == target {
				return pid
			}
		}
	}

	return 0
}

// isContinuationLine detects stack trace and multi-line error continuation lines.
// Note: multi-line JSON/object grouping is primarily handled by depth tracking
// in timestampWriter. This function catches patterns that appear at depth=0.
func isContinuationLine(line string) bool {
	if len(line) == 0 {
		return false
	}
	// Lines starting with whitespace (indented JSON, stack traces, etc.)
	if line[0] == ' ' || line[0] == '\t' {
		return true
	}
	trimmed := strings.TrimSpace(line)
	// Stack trace lines
	if strings.HasPrefix(trimmed, "at ") {
		return true
	}
	// Closing braces/brackets (end of multi-line blocks)
	if trimmed == "}" || trimmed == "})" || trimmed == "});" ||
		trimmed == "]" || trimmed == "]," || trimmed == "}," {
		return true
	}
	// Node.js error object properties
	if strings.HasPrefix(trimmed, "code:") || strings.HasPrefix(trimmed, "errno:") ||
		strings.HasPrefix(trimmed, "syscall:") || strings.HasPrefix(trimmed, "address:") ||
		strings.HasPrefix(trimmed, "port:") {
		return true
	}
	return false
}
