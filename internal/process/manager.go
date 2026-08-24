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

	// Replacements driven by the memory watchdog, counted apart from
	// RestartCount because they are planned recycles rather than crashes. A
	// caller reading "restarts: 40" should not be looking at an application
	// that has never actually failed.
	MemoryRecycles int `json:"memory_recycles"`
}

// managed represents a single running process instance.
type managed struct {
	config    *config.ProcessConfig
	cmd       *exec.Cmd
	pid       int
	port      int
	instance  int // instance index (0, 1, ...)
	status    string
	startedAt time.Time
	restarts  int
	stopCh    chan struct{}

	// The key this process is registered under in m.processes, and the single
	// source of truth for it. It used to be a parameter captured by value into
	// the monitor goroutines, which meant a blue-green promotion — a map move
	// from "app:deploy:0" to "app" — could not reach them: they went on naming
	// the old key for the rest of the process's life, and the first memory
	// recycle re-registered the live process back under it while stranding the
	// promoted entry, dead, under the real one. Every monitor now reads this
	// field instead. Written only under m.mu.
	key string

	// Set by monitorMemory immediately before it kills the process, so the
	// monitor goroutine can tell a planned recycle from a crash when Wait()
	// returns. Read and cleared under m.mu by monitor.
	recycling bool

	// True while this process is a replacement that has been started but not yet
	// promoted. Its own memory watchdog must leave it alone for that window: it
	// inherits a ceiling it may already exceed, and letting it recycle itself
	// mid-handover means the replacement kills itself while the process it was
	// meant to relieve is still waiting to be stopped. Written under m.mu.
	pendingHandover bool

	// Whether this process was started with the SO_REUSEPORT shim, and can
	// therefore be handed over rather than replaced in place.
	//
	// This is not the same question as "is a shim configured now". Upgrading the
	// daemon does not restart the processes it adopts, so a box that has just
	// been upgraded is full of processes that bound their port without
	// SO_REUSEPORT. Nothing can bind beside those, and attempting it anyway would
	// abort every time — which, because an aborted handover deliberately does not
	// fall back, would leave them growing forever until the kernel picked a
	// victim. They keep being recycled the old way until their next deployment.
	hasReusePort bool

	// Lifetime count of memory-driven recycles, carried across restarts the
	// same way restarts is.
	memoryRecycles int
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

	// Memory-limit enforcement (max_memory_restart). Defaulted in NewManager and
	// overridable per-instance in tests. readMemory is injectable so tests can
	// script RSS without a real /proc; onMemoryLimit is an observability hook.
	memInterval   time.Duration
	memWarmup     time.Duration
	memBreaches   int
	memFloor      uint64
	readMemory    func(pid int) uint64
	onMemoryLimit func(name string, rss, limit uint64)

	// Path to the SO_REUSEPORT shim, injected into Node processes via
	// NODE_OPTIONS so a replacement can bind the port its predecessor is still
	// serving. Empty disables the handover and every recycle falls back to the
	// in-place restart, which is the pre-existing behaviour.
	reusePortShim string

	// One handover at a time for the whole server. A handover means two copies
	// of an application are resident at once; letting several run together
	// multiplies that against the same RAM, and the kernel OOM killer does not
	// pick the process that caused the problem.
	recycleSlot chan struct{}

	// How long a replacement gets to come up before the handover is abandoned
	// and the old process is left serving, and how long it must stay up after
	// that before the old one is stopped.
	recycleHealthTimeout time.Duration
	recycleSettle        time.Duration
	onRecycle            func(name string, event string, detail string)
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

		// Memory-limit enforcement defaults. An instance must be up for memWarmup
		// AND over its limit for memBreaches consecutive samples before a restart,
		// so boot spikes and short GC-precollection bursts never trigger one.
		memInterval: 5 * time.Second,
		memWarmup:   60 * time.Second,
		memBreaches: 3,
		memFloor:    32 * 1024 * 1024, // ignore any limit below 32MB (likely a mistake)
		readMemory:  getProcessMemory,

		recycleSlot:          make(chan struct{}, 1),
		recycleHealthTimeout: 30 * time.Second,
		recycleSettle:        2 * time.Second,
	}
}

// SetReusePortShim enables the zero-downtime memory recycle by pointing at the
// NODE_OPTIONS shim that makes Node listeners set SO_REUSEPORT. With no shim a
// replacement cannot bind a port its predecessor still holds, so recycles stay
// on the in-place path.
func (m *Manager) SetReusePortShim(path string) {
	m.reusePortShim = path
}

// OnRecycle registers an observability hook for the handover. Set once at
// startup, before any process starts, so it can be read lock-free.
func (m *Manager) OnRecycle(fn func(name, event, detail string)) {
	m.onRecycle = fn
}

// SetRecycleHealthTimeout bounds how long a replacement gets to come up before
// the handover is abandoned and the old process is left serving.
func (m *Manager) SetRecycleHealthTimeout(d time.Duration) {
	m.recycleHealthTimeout = d
}

// SetRecycleSettle sets how long a replacement must hold after coming online
// before the process it replaces is stopped.
func (m *Manager) SetRecycleSettle(d time.Duration) {
	m.recycleSettle = d
}

func (m *Manager) notifyRecycle(name, event, detail string) {
	if m.onRecycle != nil {
		m.onRecycle(name, event, detail)
	}
}

// OnStatusChange registers a callback for process status changes.
func (m *Manager) OnStatusChange(fn func(name, status string)) {
	m.onStatusChange = fn
}

// OnMemoryLimit registers a callback fired when a process is restarted for
// exceeding its configured max_memory. Set once at startup before any process
// starts, so the sampler reads it lock-free (like onStatusChange).
func (m *Manager) OnMemoryLimit(fn func(name string, rss, limit uint64)) {
	m.onMemoryLimit = fn
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
			// Signal stop intent before the kill. Without it the monitor (or
			// monitorAdopted) goroutine sees the process die with stopCh still
			// open and starts it again from its saved config — so replacing a
			// process left two of them on one port. That is what a daemon upgrade
			// did to a drifted server: the old instance was resurrected onto the
			// same port the freshly started one had just taken.
			m.signalStop(proc)
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
	previousRecycles := 0
	if existing, ok := m.processes[key]; ok {
		if existing.status == StatusStopped || existing.status == StatusErrored {
			// Dead process - clean start, reset counter, remove stale entry
			previousRestarts = 0
			previousRecycles = 0
			m.store.DeleteProcess(key)
		} else {
			previousRestarts = existing.restarts
			previousRecycles = existing.memoryRecycles
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

	// Make this listener shareable so its eventual replacement can bind the same
	// port while it is still serving. This has to be set from the very first
	// start: SO_REUSEPORT only works when every socket on the port sets it, so a
	// process started without the shim can never be handed over, only restarted
	// in place.
	env, reusePortEnabled := withReusePortShim(env, cfg, m.reusePortShim)

	// Drop to the configured user, if there is one. Resolved before the process
	// is built so an unknown user fails here rather than starting something as
	// root and reporting success.
	credential, err := credentialFor(cfg.User)
	if err != nil {
		return fmt.Errorf("resolve user for %s: %w", cfg.Name, err)
	}

	if credential != nil {
		// HOME still says /root at this point, inherited from the daemon. A
		// process that cannot write its own cache directory fails in a way that
		// looks like a broken release, so the identity variables move with the
		// credentials.
		env = environForUser(env, cfg.User)
	}

	// Build command - use shell to handle complex commands
	cmd := exec.Command("sh", "-c", cfg.Command)
	cmd.Dir = cfg.CWD // Symlink path, NOT resolved
	cmd.Env = env

	// Set process group so we can kill the whole tree
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:    true,
		Credential: credential,
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
		config:         cfg,
		cmd:            cmd,
		pid:            cmd.Process.Pid,
		port:           port,
		instance:       instance,
		status:         StatusStarting,
		startedAt:      time.Now(),
		restarts:       previousRestarts,
		memoryRecycles: previousRecycles,
		stopCh:         make(chan struct{}),
		key:            key,
		hasReusePort:   reusePortEnabled,
	}

	m.processes[key] = proc

	// Persist state
	m.persistProcess(proc, key)

	// Monitor process in background. Neither monitor takes the key: they read
	// proc.key so a promotion reaches them.
	go m.monitor(proc, logFile, errFile)

	// Enforce max_memory if a real limit is configured.
	if limit := parseMemoryLimit(cfg, m.memFloor); limit > 0 {
		go m.monitorMemory(proc, limit)
	}

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

// reclaimDeployKeys moves any process of this name that is registered under one
// of the deploy keys the next Deploy will claim back onto its final key.
//
// This is the repair path for state the promotion bug already created. A process
// stranded on "app:deploy:0" is adopted under that same key when the daemon
// restarts, so upgrading the binary does not heal it — and the next deploy would
// hand that key to startInstance, whose prologue stops whatever it finds there.
// The process it would find is the one serving the site.
//
// A dead twin sitting on the final key is dropped; a live one is left alone and
// handled as an ordinary old worker, because two live processes for one name is a
// question about ports, and this is not the place to answer it.
func (m *Manager) reclaimDeployKeys(name string, workerCount int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := 0; i < workerCount; i++ {
		deployKey := fmt.Sprintf("%s:deploy:%d", name, i)
		proc, ok := m.processes[deployKey]
		if !ok || proc.config == nil || proc.config.Name != name {
			continue
		}

		finalKey := instanceKey(name, proc.instance, proc.config.Instances)
		if finalKey == deployKey {
			continue
		}

		if existing, taken := m.processes[finalKey]; taken && existing != proc {
			if processAlive(existing.pid) {
				continue
			}
			delete(m.processes, finalKey)
			m.store.DeleteProcess(finalKey)
		}

		delete(m.processes, deployKey)
		m.store.DeleteProcess(deployKey)
		proc.key = finalKey
		m.processes[finalKey] = proc
		m.persistProcess(proc, finalKey)
	}
}

// Deploy performs a blue-green deployment: starts new workers on new ports,
// waits for them to be online, then gracefully shuts down old workers.
// Old workers continue serving traffic until new workers are confirmed healthy,
// ensuring zero-downtime.
func (m *Manager) Deploy(cfg *config.ProcessConfig, newPorts []int) (*DeployResult, error) {
	workerCount := cfg.ResolveWorkerCount()

	// 0. Free the deploy keys this call is about to claim. A daemon that was
	//    upgraded while a process sat on its deploy key — the state the drift bug
	//    left behind across the fleet — would otherwise have step 2 kill the live
	//    site as a side effect of "starting the new worker", because
	//    startInstance stops whatever already holds the key it is given.
	m.reclaimDeployKeys(cfg.Name, workerCount)

	// 1. Collect the workers this deploy will replace, as POINTERS.
	//    Keys were used here once, and a key is not a stable identity: step 2
	//    reuses the deploy keys, so a key collected here could name the brand new
	//    process by the time step 4 looked it up — which is exactly how a deploy
	//    came to drain the worker it had just promoted.
	m.mu.RLock()
	var oldWorkers []*managed
	var oldPorts []int
	for _, proc := range m.processes {
		if proc.config.Name == cfg.Name {
			oldWorkers = append(oldWorkers, proc)
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

	// 4. Promote: move the new workers from their deploy keys to their final keys.
	//    This runs BEFORE the old workers are unregistered. The old order — collect
	//    old workers, then promote — read the map by key, and because step 2 reuses
	//    the deploy keys, a stale deploy key in oldKeys pointed at the process that
	//    had just been started. The deploy deleted it, promoted nothing, and parked
	//    the live worker for the drain that follows the nginx switch. That is the
	//    commerce-v2 outage: no listener on the new port, no entry in the manager,
	//    and a deployment recorded successful.
	m.mu.Lock()
	promoted := make(map[*managed]bool)
	finalKeys := make(map[string]bool)
	for i := 0; i < workerCount; i++ {
		deployKey := fmt.Sprintf("%s:deploy:%d", cfg.Name, i)
		finalKey := instanceKey(cfg.Name, i, workerCount)
		proc, ok := m.processes[deployKey]
		if !ok {
			continue
		}
		delete(m.processes, deployKey)
		m.store.DeleteProcess(deployKey)
		proc.key = finalKey
		m.processes[finalKey] = proc
		m.persistProcess(proc, finalKey)
		promoted[proc] = true
		finalKeys[finalKey] = true
	}

	// Now unregister the workers this deploy replaces, identified by pointer. A
	// promoted worker is never one of them, however the keys happen to line up.
	type oldWorker struct {
		proc *managed
		key  string
	}
	var retired []oldWorker
	for _, proc := range oldWorkers {
		if promoted[proc] {
			continue
		}
		key := proc.key
		if current, ok := m.processes[key]; ok && current == proc {
			delete(m.processes, key)
		}
		retired = append(retired, oldWorker{proc: proc, key: key})
	}
	m.mu.Unlock()

	// 5. Store old workers for explicit drain by caller (Depfloy)
	// Old workers keep running until Drain() is called after nginx switch
	if len(retired) > 0 {
		m.mu.Lock()
		for _, ow := range retired {
			m.pendingDrain[cfg.Name] = append(m.pendingDrain[cfg.Name], ow.proc)
			// Do NOT delete a store key that step 4 just re-persisted for a promoted
			// worker. An old worker and its replacement share the same final key
			// (same name + worker count), so deleting it here would wipe the live
			// process from the store and orphan it on the next daemon restart.
			if !finalKeys[ow.key] {
				m.store.DeleteProcess(ow.key)
			}
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
	parked, ok := m.pendingDrain[name]
	if ok {
		delete(m.pendingDrain, name)
	}

	// Never drain a process that is still the managed instance. Depfloy calls
	// Drain after it has already pointed nginx at the new port, so killing a live
	// worker here is an outage with no way back — the site stays down until a
	// human redeploys, and the deployment has already been recorded successful.
	// Deploy is supposed to park only retired workers; this is the check that
	// keeps a bug there from reaching production as a dead site.
	var workers []*managed
	for _, proc := range parked {
		if m.memInstanceManaged(proc) {
			continue
		}
		workers = append(workers, proc)
	}

	// Signal stop intent under the lock so the mutation of proc.status/stopCh does
	// not race with the parked workers' still-running monitor goroutines.
	for _, proc := range workers {
		m.signalStop(proc)
	}
	m.mu.Unlock()

	if len(workers) == 0 {
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
		// Deduplicate ports. Start below derives its worker count from
		// len(ports), so a name that appears twice on the SAME port — one
		// application recorded twice, which is what promotion drift leaves —
		// would come back as two instances fighting over one port. A genuine
		// multi-worker process has a distinct port per worker and is unaffected.
		//
		// This is the path Depfloy's own upgrade takes: DpmUpgradeService runs
		// `dpm reload` after installing the binary, so the fault landed on a
		// customer box before this guard existed.
		if !containsPortValue(saved[name].ports, proc.port) {
			saved[name].ports = append(saved[name].ports, proc.port)
		}
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

	// Old blue-green workers parked in pendingDrain (Drain not yet called) are
	// NOT in m.processes, so Stop() above misses them. Their monitor goroutines
	// are still live and would resurrect the process (RestartPolicy "always")
	// when it next exits — silently undoing the delete (Server 46 incident,
	// DP-98). Signal-stop them under the lock so the monitors see the stop
	// intent, then reap them lock-free below.
	m.mu.Lock()
	parked := m.pendingDrain[name]
	delete(m.pendingDrain, name)
	for _, proc := range parked {
		m.signalStop(proc)
	}

	for key, proc := range m.processes {
		if proc.config.Name == name {
			delete(m.processes, key)
			m.store.DeleteProcess(key)
		}
	}
	m.mu.Unlock()

	// Blocking kill/wait outside the lock (see stopProcess docs).
	for _, proc := range parked {
		m.stopProcess(proc)
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
			Name:           instanceKey(proc.config.Name, proc.instance, proc.config.Instances),
			PID:            proc.pid,
			Status:         proc.status,
			Port:           proc.port,
			Type:           proc.config.Type,
			Memory:         getProcessMemory(proc.pid),
			RestartCount:   proc.restarts,
			MemoryRecycles: proc.memoryRecycles,
			StartedAt:      proc.startedAt,
			Command:        proc.config.Command,
			CWD:            proc.config.CWD,
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
				Name:           instanceKey(proc.config.Name, proc.instance, proc.config.Instances),
				PID:            proc.pid,
				Status:         proc.status,
				Port:           proc.port,
				Type:           proc.config.Type,
				Memory:         getProcessMemory(proc.pid),
				RestartCount:   proc.restarts,
				MemoryRecycles: proc.memoryRecycles,
				StartedAt:      proc.startedAt,
				Command:        proc.config.Command,
				CWD:            proc.config.CWD,
				Env:            proc.config.Env,
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

	proc.key = ps.Name
	m.processes[ps.Name] = proc

	// Monitor the re-adopted process
	go m.monitorAdopted(proc)

	// Enforce max_memory if a real limit is configured. The warm-up window gives
	// adopted processes a fresh grace period after a daemon restart.
	if limit := parseMemoryLimit(&cfg, m.memFloor); limit > 0 {
		go m.monitorMemory(proc, limit)
	}

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
		// Send the configured stop signal to the whole process group. We deliberately
		// do NOT call cmd.Wait() here: the monitor goroutine spawned in startInstance
		// already owns the single cmd.Wait() for this Cmd, and a second concurrent
		// Wait() on the same *exec.Cmd is a data race. We poll for the process to
		// disappear instead; the monitor reaps it when it exits (cmd.WaitDelay bounds
		// that even if a grandchild keeps the output pipe open).
		pgid, err := syscall.Getpgid(proc.pid)
		if err == nil {
			syscall.Kill(-pgid, stopSig)
		} else {
			syscall.Kill(proc.pid, stopSig)
		}

		// Wait for graceful shutdown up to stopTimeout.
		if !waitForExit(proc.pid, stopTimeout) {
			// Force kill the whole process group, then give the monitor a bounded
			// window (WaitDelay + margin) to reap so stopProcess never blocks forever.
			if pgid > 0 {
				syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				syscall.Kill(proc.pid, syscall.SIGKILL)
			}
			waitDelay := resolveTimeout(proc.config.WaitDelay, 10*time.Second)
			waitForExit(proc.pid, waitDelay+2*time.Second)
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

// withReusePortShim adds the SO_REUSEPORT shim to NODE_OPTIONS for Node
// processes, preserving anything the customer already put there — their flags
// are part of how their application boots, and dropping them would be a silent
// behaviour change that only shows up under load.
// Reports whether the process will actually carry SO_REUSEPORT, which is what
// decides if it can later be handed over instead of replaced in place.
func withReusePortShim(env []string, cfg *config.ProcessConfig, shim string) ([]string, bool) {
	if shim == "" || !strings.EqualFold(cfg.Type, "nodejs") {
		return env, false
	}

	const key = "NODE_OPTIONS="
	addition := "--require " + shim

	for i, entry := range env {
		if !strings.HasPrefix(entry, key) {
			continue
		}
		existing := strings.TrimPrefix(entry, key)
		if strings.Contains(existing, shim) {
			return env, true // already carries it; a restart must not stack copies
		}
		if strings.TrimSpace(existing) == "" {
			env[i] = key + addition
		} else {
			env[i] = key + existing + " " + addition
		}
		return env, true
	}

	return append(env, key+addition), true
}

// currentKey returns the key this process is registered under. Read it fresh
// every time rather than caching it across an unlock: a blue-green promotion
// moves a process from its deploy key to its final key, and a monitor holding
// the old string would re-register the process under a key nothing serves.
func (m *Manager) currentKey(proc *managed) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return proc.key
}

// monitor watches a started process for exit and handles restarts.
//
// The key is deliberately NOT a parameter. It used to be, and a promotion could
// not reach the running goroutine, which is what stranded promoted workers under
// their deploy keys and eventually let a deploy drain the process it had just
// started. proc.key is the only source of truth.
func (m *Manager) monitor(proc *managed, logFile, errFile io.Closer) {
	defer logFile.Close()
	defer errFile.Close()

	// Panic recovery - never crash the daemon
	defer func() {
		if r := recover(); r != nil {
			m.mu.Lock()
			proc.status = StatusErrored
			m.persistProcess(proc, proc.key)
			m.mu.Unlock()
		}
	}()

	// Mark as online after brief startup period
	time.Sleep(2 * time.Second)
	if processAlive(proc.pid) {
		m.mu.Lock()
		proc.status = StatusOnline
		key := proc.key
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

	// A memory recycle is a replacement we asked for, not a failure the process
	// had. Treating the two the same is what kept single-instance sites down:
	// the crash backoff reaches 30s past ten restarts, and the process was
	// serving nothing for all of it, while the crash budget counted down to a
	// maxRestarts that stopped the application permanently. An application that
	// simply needs more memory than its ceiling would eventually just stay
	// dead, with the last thing in the log being a restart that worked.
	recycle := proc.recycling
	proc.recycling = false

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

	// The crash guards below are deliberately skipped for a recycle, but the
	// restart policy above is not: "never" still means never, however the
	// process came to exit.
	if !recycle {
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
	}

	if !shouldRestart {
		proc.status = StatusStopped
		key := proc.key
		m.persistProcess(proc, key)
		m.mu.Unlock()
		m.notifyStatusChange(key, StatusStopped)
		return
	}

	var delay time.Duration
	if recycle {
		// No backoff: the replacement is wanted immediately, and every
		// millisecond here is a millisecond of 502s for a single-instance app.
		// Recycles cannot run away — monitorMemory will not fire again until
		// the new process has cleared its warm-up window.
		proc.memoryRecycles++
		delay = recycleSettleDelay
	} else {
		// Restart with exponential backoff
		proc.restarts++
		delay = restartBackoff(proc.restarts)
	}
	proc.status = StatusStarting
	m.persistProcess(proc, proc.key)
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

	// Read the key after the sleep, not before: a promotion during the delay
	// must be honoured, or the replacement lands on a key nothing serves.
	key := m.currentKey(proc)
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
func (m *Manager) monitorAdopted(proc *managed) {
	defer func() {
		if r := recover(); r != nil {
			m.mu.Lock()
			proc.status = StatusErrored
			m.persistProcess(proc, proc.key)
			m.mu.Unlock()
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
				key := proc.key
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

// waitForExit polls until the process is gone or the timeout elapses, returning
// true if it exited. Used by stopProcess so it never calls cmd.Wait() itself -
// the monitor goroutine owns the single Wait() for a cmd-backed process.
func waitForExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !processAlive(pid) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// parseMemoryLimit converts a process's configured max_memory ("512MB", "1GB")
// to bytes. Unlike dpmlog.ParseMaxSize (which falls back to 100MB on a bad
// value), it returns 0 = "no limit / disabled" for a nil/empty/unparseable/zero
// value or one below floor — so a process without a real limit is never enforced
// by accident, and a typo like "0" or "5MB" disables enforcement rather than
// imposing a dangerously low cap.
func parseMemoryLimit(cfg *config.ProcessConfig, floor uint64) uint64 {
	if cfg == nil || cfg.Resources == nil {
		return 0
	}
	s := strings.TrimSpace(strings.ToUpper(cfg.Resources.MaxMemory))
	if s == "" {
		return 0
	}

	var multiplier uint64 = 1
	switch {
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}

	val, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil || val == 0 {
		return 0
	}
	limit := val * multiplier
	if limit < floor {
		return 0
	}
	return limit
}

// memInstanceManaged reports whether proc is still an active managed instance
// (a value in m.processes), as opposed to one parked in pendingDrain during a
// blue-green deploy or superseded by a redeploy. The caller MUST hold m.mu.
// The memory sampler uses this so it never acts on a drained/replaced worker —
// crucially, never resurrecting a parked worker (DP-98).
func (m *Manager) memInstanceManaged(proc *managed) bool {
	for _, p := range m.processes {
		if p == proc {
			return true
		}
	}
	return false
}

// monitorMemory samples a process's RSS and restarts it once it stays above its
// configured max_memory for memBreaches consecutive samples. One goroutine per
// instance, mirroring monitor/monitorAdopted, tied to proc.stopCh so Stop/
// Restart/Delete/Drain tear it down for free. The hysteresis counter is
// goroutine-local, so there is no new shared state.
//
// Mutex discipline (load-bearing — holding m.mu across /proc or a kill has
// deadlocked the daemon before): each tick takes only a brief RLock to snapshot
// proc.pid and confirm proc is still managed, then releases it before the /proc
// read and the (blocking) stopProcess call.
func (m *Manager) monitorMemory(proc *managed, limit uint64) {
	defer func() { _ = recover() }()

	// Warm-up grace: never enforce during boot / JIT / first-request compile
	// spikes. Counted from goroutine start, which also gives an adopted process a
	// fresh grace window after a daemon restart. Abortable by an intentional stop.
	select {
	case <-proc.stopCh:
		return
	case <-time.After(m.memWarmup):
	}

	ticker := time.NewTicker(m.memInterval)
	defer ticker.Stop()

	over := 0
	for {
		select {
		case <-proc.stopCh:
			return
		case <-ticker.C:
			// Snapshot under a brief read lock, then release before any I/O.
			m.mu.RLock()
			tracked := m.memInstanceManaged(proc)
			pid := proc.pid
			pending := proc.pendingHandover
			m.mu.RUnlock()

			if !tracked {
				return // drained, deleted, or replaced by a redeploy
			}

			if pending {
				// A replacement mid-handover. It is not serving on its own terms
				// yet and must not recycle itself out from under the handover.
				over = 0
				continue
			}

			rss := m.readMemory(pid)
			// A zero read (process exited between snapshot and read, or non-Linux)
			// is treated as under-limit so a race never counts as a breach.
			if rss == 0 || rss <= limit {
				over = 0
				continue
			}

			over++
			if over < m.memBreaches {
				continue
			}

			// Sustained breach.
			select {
			case <-proc.stopCh:
				return
			default:
			}

			m.mu.RLock()
			key := proc.key
			m.mu.RUnlock()
			if m.onMemoryLimit != nil {
				m.onMemoryLimit(key, rss, limit)
			}

			// Preferred path: bring the replacement up on the same port before
			// this process goes away, so the port is never unserved.
			switch m.recycleInstance(proc) {
			case recycleHandedOver:
				return
			case recycleKeptRunning:
				// A replacement was attempted and this process is still the one
				// serving — either it could not come up, or another handover had
				// the slot. Do NOT replace it in place: that would stop a working
				// process to start one already shown not to boot. Reset the breach
				// counter so it takes another sustained breach to try again.
				over = 0
				continue
			case recycleUnavailable:
				// The handover was never attempted, so the in-place restart below
				// is still the only way to reclaim the memory.
			}

			// Fall back to replacing in place. Re-check stop intent and
			// management, then kill WITHOUT signalStop: the monitor goroutine
			// sees the process die with stopCh still open and starts the
			// replacement via the restart_policy path.
			select {
			case <-proc.stopCh:
				return
			default:
			}
			// Mark the kill as planned before it happens. The monitor
			// goroutine reads this the moment Wait() returns, so setting it
			// afterwards would race and the recycle would be charged to the
			// crash budget. Full lock, not RLock: this writes to proc.
			m.mu.Lock()
			tracked = m.memInstanceManaged(proc)
			if tracked {
				proc.recycling = true
			}
			m.mu.Unlock()
			if !tracked {
				return
			}

			m.stopProcess(proc)
			return
		}
	}
}

// recycleInstance replaces a process that has outgrown its memory ceiling
// without ever leaving its port unserved.
//
// The old way was to kill the process and start its replacement on the freed
// port. For a single-instance application behind nginx that is an outage for the
// whole boot: with one server in the upstream, nginx's proxy_next_upstream retry
// has nowhere to go and answers 502. Measured at 200 rps in
// depfloy-app/docker/testbed/reuseport, that is ~52 lost requests per recycle
// against a lab app that boots in 2ms — a real storefront boots in seconds, and
// app_244 recycled 196 times.
//
// Here the replacement binds the same port alongside the process still serving
// it (SO_REUSEPORT, via the NODE_OPTIONS shim), and only once it is up is the
// old one stopped. nginx is never reconfigured and no port is reallocated,
// because the port never changes. Requests that hit the old process as it goes
// away fail retryably — recv() failed, upstream prematurely closed — and the
// retry lands on the replacement already listening there. The same measurements
// show zero lost requests, including when the application ignores SIGTERM and
// has to be killed.
//
// The three outcomes are deliberately distinct. "Unavailable" means the handover
// was never attempted, so the caller may still replace the process in place.
// "Kept running" means a replacement was started and would not come up — falling
// back there would kill a serving process in order to start one already proven
// unable to boot, which is how a bad release turns into an outage.
type recycleOutcome int

const (
	recycleUnavailable recycleOutcome = iota
	recycleHandedOver
	recycleKeptRunning
)

func (m *Manager) recycleInstance(proc *managed) recycleOutcome {
	if m.reusePortShim == "" {
		return recycleUnavailable
	}

	m.mu.RLock()
	key := proc.key
	cfg := proc.config
	port := proc.port
	instance := proc.instance
	managedNow := m.memInstanceManaged(proc)
	canShare := proc.hasReusePort
	m.mu.RUnlock()

	if !managedNow || cfg == nil || port <= 0 {
		return recycleUnavailable
	}

	// This process bound its port without SO_REUSEPORT — it predates the shim, or
	// was adopted across a daemon upgrade, which does not restart what it adopts.
	// Nothing can bind beside it, so the only way to reclaim its memory is the
	// in-place restart, until its next deployment starts it with the shim.
	if !canShare {
		return recycleUnavailable
	}
	name := cfg.Name

	// One overlap at a time for the whole server. Anything already handing over
	// gets to finish; this process stays over its ceiling until the next sample,
	// which is a far cheaper outcome than two doubled applications at once.
	select {
	case m.recycleSlot <- struct{}{}:
	default:
		m.notifyRecycle(name, "deferred", "another handover is in progress")
		// Not "unavailable": the process is fine, it just has to wait its turn.
		// Replacing it in place now would be an outage taken to avoid a queue.
		return recycleKeptRunning
	}
	defer func() { <-m.recycleSlot }()

	// Re-check under the lock: the wait for the slot is unbounded in principle,
	// and a redeploy or a stop may have replaced this instance meanwhile.
	m.mu.RLock()
	stillManaged := m.memInstanceManaged(proc)
	m.mu.RUnlock()
	if !stillManaged {
		return recycleKeptRunning
	}
	select {
	case <-proc.stopCh:
		return recycleKeptRunning
	default:
	}

	replacementKey := key + ":recycle"
	m.notifyRecycle(name, "starting", fmt.Sprintf("replacement on port %d", port))

	if err := m.startInstance(cfg, replacementKey, instance, port); err != nil {
		// The usual cause is a process started before the shim existed, whose
		// socket does not carry SO_REUSEPORT — so nothing can bind beside it.
		m.notifyRecycle(name, "unavailable", err.Error())
		return recycleUnavailable
	}

	m.mu.Lock()
	replacement := m.processes[replacementKey]
	if replacement != nil {
		// Take the replacement out of the watchdog's reach until it is promoted.
		// It carries the same ceiling as the process it is relieving and may be
		// over it from the first sample, and a replacement that recycles itself
		// mid-handover leaves nobody to promote.
		replacement.pendingHandover = true
	}
	m.mu.Unlock()
	if replacement == nil {
		return recycleUnavailable
	}

	if !m.awaitReplacement(replacement) {
		// Never trade a running site for a replacement that will not come up.
		m.notifyRecycle(name, "aborted", "replacement did not come up; keeping the running process")
		m.mu.Lock()
		if current, ok := m.processes[replacementKey]; ok && current == replacement {
			delete(m.processes, replacementKey)
		}
		m.store.DeleteProcess(replacementKey)
		replacement.pendingHandover = false
		m.signalStop(replacement)
		m.mu.Unlock()
		m.stopProcess(replacement)
		return recycleKeptRunning
	}

	// The replacement is serving the port. Promote it onto the real key and take
	// the old process out of the manager in the same critical section, so no
	// reader ever sees two live instances under one name.
	m.mu.Lock()
	delete(m.processes, replacementKey)
	m.store.DeleteProcess(replacementKey)
	replacement.key = key
	replacement.memoryRecycles = proc.memoryRecycles + 1
	replacement.restarts = proc.restarts
	replacement.pendingHandover = false
	m.processes[key] = replacement
	m.persistProcess(replacement, key)
	// signalStop before the kill so the old process's monitor treats the exit as
	// intentional and does not resurrect it onto a key the replacement now owns.
	m.signalStop(proc)
	m.mu.Unlock()

	m.stopProcess(proc)
	m.notifyRecycle(name, "completed", fmt.Sprintf("port %d handed over", port))
	return recycleHandedOver
}

// awaitReplacement waits for a freshly started replacement to reach online and
// hold there. It reports false the moment the process dies, so a release that
// crashes on boot costs nothing rather than taking the site with it.
func (m *Manager) awaitReplacement(replacement *managed) bool {
	deadline := time.Now().Add(m.recycleHealthTimeout)
	online := false

	for time.Now().Before(deadline) {
		if !processAlive(replacement.pid) {
			return false
		}
		m.mu.RLock()
		status := replacement.status
		tracked := m.memInstanceManaged(replacement)
		m.mu.RUnlock()
		if !tracked {
			return false
		}
		if status == StatusOnline {
			online = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !online {
		return false
	}

	// A process that comes up and immediately falls over must not be promoted;
	// the old one is still serving and is the safer thing to keep.
	settleDeadline := time.Now().Add(m.recycleSettle)
	for time.Now().Before(settleDeadline) {
		if !processAlive(replacement.pid) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}

	return processAlive(replacement.pid)
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
// How long to wait between a memory recycle's kill and its replacement.
// stopProcess already waits for the process to exit and reaps whatever still
// holds the port, so this is only a settling margin for the kernel to release
// the listening socket — not a backoff. Anything longer is downtime.
const recycleSettleDelay = 250 * time.Millisecond

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

// containsPortValue reports whether ports already holds p.
func containsPortValue(ports []int, p int) bool {
	for _, existing := range ports {
		if existing == p {
			return true
		}
	}
	return false
}
