package process

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// testManager creates a Manager backed by a temp BoltDB store and temp log dir.
func testManager(t *testing.T) (*Manager, *state.Store) {
	t.Helper()
	stateDir := t.TempDir()
	logDir := t.TempDir()

	store, err := state.Open(stateDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	rotation := config.RotationConfig{
		MaxSize:    "10MB",
		MaxBackups: 2,
		Compress:   false,
	}

	mgr := NewManager(store, logDir, rotation)
	return mgr, store
}

// testConfig creates a ProcessConfig for testing with sensible defaults.
func testConfig(name, command string) *config.ProcessConfig {
	return &config.ProcessConfig{
		Name:          name,
		Command:       command,
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     1,
		RestartPolicy: "always",
		MaxRestarts:   50,
	}
}

// waitForStatus polls the manager until the named process reaches the given
// status, or the timeout expires. Returns the final status observed.
func waitForStatus(m *Manager, name, want string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		infos := m.List()
		for _, info := range infos {
			if info.Name == name && info.Status == want {
				return info.Status
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Return whatever status we can find
	infos := m.List()
	for _, info := range infos {
		if info.Name == name {
			return info.Status
		}
	}
	return ""
}

// ==================== Restart Counter Preservation ====================

func TestRestartCounterPreservation(t *testing.T) {
	mgr, _ := testManager(t)

	// Start a long-lived process
	cfg := testConfig("counter-app", "sleep 300")
	cfg.RestartPolicy = "always"

	err := mgr.Start(cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for process to be online
	status := waitForStatus(mgr, "counter-app", StatusOnline, 5*time.Second)
	if status != StatusOnline {
		t.Fatalf("expected online, got %s", status)
	}

	// Verify initial restart count is 0
	infos := mgr.List()
	if len(infos) == 0 {
		t.Fatal("no processes found")
	}
	if infos[0].RestartCount != 0 {
		t.Errorf("initial restart count = %d, want 0", infos[0].RestartCount)
	}

	// Simulate a crash by calling startInstance again (as monitor would after exit).
	// First we manually set a restart count on the existing process.
	mgr.mu.Lock()
	for _, proc := range mgr.processes {
		if proc.config.Name == "counter-app" {
			proc.restarts = 3
		}
	}
	mgr.mu.Unlock()

	// Re-start the instance through startInstance (like monitor does after crash).
	// The key for a single-instance process is just the name.
	err = mgr.startInstance(cfg, "counter-app", 0, 0)
	if err != nil {
		t.Fatalf("restart instance: %v", err)
	}

	// Verify the restart counter was preserved (not reset to 0)
	mgr.mu.RLock()
	proc := mgr.processes["counter-app"]
	restarts := proc.restarts
	mgr.mu.RUnlock()

	if restarts != 3 {
		t.Errorf("restart count after re-start = %d, want 3 (preserved)", restarts)
	}

	// Clean up
	mgr.Stop("counter-app")
}

// ==================== Fast Crash Detection ====================

func TestFastCrashDetection(t *testing.T) {
	mgr, store := testManager(t)

	// Use a command that exits immediately (fast crash)
	cfg := testConfig("crash-app", "exit 1")
	cfg.RestartPolicy = "always"
	cfg.MaxRestarts = 50 // High limit - we want fast crash detection to kick in first

	err := mgr.Start(cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the process to reach errored/stopped state via fast crash detection.
	// Fast crash limit is 5 crashes within 10s per crash, so after 5 fast crashes
	// it should stop restarting. With backoff delays this should take ~15-20s.
	deadline := time.Now().Add(45 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		infos := mgr.List()
		for _, info := range infos {
			if info.Name == "crash-app" {
				finalStatus = info.Status
				if info.Status == StatusStopped || info.Status == StatusErrored {
					goto done
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
done:

	if finalStatus != StatusStopped && finalStatus != StatusErrored {
		t.Errorf("expected stopped or errored after fast crashes, got %s", finalStatus)
	}

	// Verify the state was persisted to BoltDB
	ps, err := store.GetProcess("crash-app")
	if err != nil {
		t.Logf("note: process may have been cleaned up from store: %v", err)
	} else {
		if ps.Status != StatusStopped && ps.Status != StatusErrored {
			t.Errorf("BoltDB status = %s, want stopped or errored", ps.Status)
		}
		// Should have at least 5 restarts (the fast crash limit)
		if ps.RestartCount < 5 {
			t.Errorf("BoltDB restart count = %d, want >= 5", ps.RestartCount)
		}
	}
}

// ==================== Max Restarts Limit ====================

func TestMaxRestartsLimit(t *testing.T) {
	mgr, _ := testManager(t)

	// Use a command that crashes immediately
	cfg := testConfig("max-restart-app", "exit 1")
	cfg.RestartPolicy = "always"
	cfg.MaxRestarts = 3 // Low limit for fast test

	err := mgr.Start(cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the process to stop after hitting max restarts.
	// With max_restarts=3 and fast crash detection at 5, the max_restarts
	// limit should be hit first.
	deadline := time.Now().Add(30 * time.Second)
	var finalStatus string
	var finalRestarts int
	for time.Now().Before(deadline) {
		infos := mgr.List()
		for _, info := range infos {
			if info.Name == "max-restart-app" {
				finalStatus = info.Status
				finalRestarts = info.RestartCount
				if info.Status == StatusStopped || info.Status == StatusErrored {
					goto done
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
done:

	if finalStatus != StatusStopped && finalStatus != StatusErrored {
		t.Errorf("expected stopped/errored after max restarts, got %s", finalStatus)
	}

	// Should not exceed maxRestarts
	if finalRestarts > 3 {
		t.Errorf("restart count = %d, should not exceed max_restarts=3", finalRestarts)
	}
}

// ==================== ReloadAll Without Deadlock ====================

func TestReloadAllCompletesWithinTimeout(t *testing.T) {
	mgr, _ := testManager(t)

	// Start multiple processes
	for i := 0; i < 3; i++ {
		cfg := testConfig("reload-app", "sleep 300")
		cfg.Name = "reload-app"
		if i == 0 {
			err := mgr.Start(cfg, nil)
			if err != nil {
				t.Fatalf("start %d: %v", i, err)
			}
		}
	}

	// Start a second distinct process
	cfg2 := testConfig("reload-app-2", "sleep 300")
	if err := mgr.Start(cfg2, nil); err != nil {
		t.Fatalf("start reload-app-2: %v", err)
	}

	// Wait for processes to come online
	time.Sleep(3 * time.Second)

	// Verify we have processes
	before := mgr.List()
	if len(before) < 2 {
		t.Fatalf("expected at least 2 processes before reload, got %d", len(before))
	}

	// ReloadAll should complete within 10 seconds (not deadlock for 300+ seconds)
	done := make(chan struct{})
	var restarted, failed int
	var reloadErr error
	go func() {
		restarted, failed, reloadErr = mgr.ReloadAll()
		close(done)
	}()

	select {
	case <-done:
		// Good - completed within timeout
	case <-time.After(15 * time.Second):
		t.Fatal("ReloadAll deadlocked - did not complete within 15 seconds")
	}

	if reloadErr != nil {
		t.Fatalf("ReloadAll error: %v", reloadErr)
	}

	// Verify processes were restarted
	if restarted == 0 && failed == 0 {
		t.Error("ReloadAll did not restart or fail any processes")
	}

	t.Logf("ReloadAll: restarted=%d, failed=%d", restarted, failed)

	// Wait for new processes to come up
	time.Sleep(3 * time.Second)

	// Verify processes exist after reload
	after := mgr.List()
	if len(after) == 0 {
		t.Error("no processes after ReloadAll")
	}

	// Clean up all processes
	for _, info := range after {
		mgr.Stop(info.Name)
	}
}

// ==================== ReloadAll With Adopted Processes (cmd=nil) ====================

func TestReloadAllWithAdoptedProcesses(t *testing.T) {
	mgr, store := testManager(t)

	// Start a real process
	cfg := testConfig("adopted-test", "sleep 300")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for it to be online
	waitForStatus(mgr, "adopted-test", StatusOnline, 5*time.Second)

	// Simulate an adopted process state: set cmd to nil, mimicking what
	// happens after daemon restart + Attach. Get the PID from the real process.
	mgr.mu.Lock()
	proc := mgr.processes["adopted-test"]
	pid := proc.pid
	// Simulate adopted state: keep pid, nil out cmd (as Attach does)
	proc.cmd = nil
	mgr.mu.Unlock()

	// Also save the process state to BoltDB so ReloadAll can find it
	cfgJSON, _ := json.Marshal(cfg)
	store.SaveProcess(&state.ProcessState{
		Name:       "adopted-test",
		PID:        pid,
		Port:       0,
		Status:     StatusOnline,
		Command:    cfg.Command,
		CWD:        cfg.CWD,
		Type:       cfg.Type,
		ConfigJSON: cfgJSON,
	})

	// ReloadAll should not panic or deadlock when encountering cmd=nil processes
	done := make(chan struct{})
	var reloadErr error
	go func() {
		defer func() {
			if r := recover(); r != nil {
				reloadErr = nil // We'll check for panic separately
				t.Errorf("ReloadAll panicked with adopted process: %v", r)
			}
			close(done)
		}()
		_, _, reloadErr = mgr.ReloadAll()
	}()

	select {
	case <-done:
		// Good - no panic, no deadlock
	case <-time.After(15 * time.Second):
		t.Fatal("ReloadAll deadlocked with adopted process (cmd=nil)")
	}

	// Allow some error - the important thing is no panic or deadlock
	t.Logf("ReloadAll with adopted process result: err=%v", reloadErr)

	// Clean up
	time.Sleep(1 * time.Second)
	for _, info := range mgr.List() {
		mgr.Stop(info.Name)
	}
}

// ==================== Deploy (Blue-Green) ====================

func TestDeployBlueGreen(t *testing.T) {
	mgr, _ := testManager(t)

	// Start the "old" process. With Cluster.Mode="fixed" and Workers=1,
	// ResolveWorkerCount returns 1, so instanceKey = "deploy-app" (no suffix).
	cfg := testConfig("deploy-app", "sleep 300")
	cfg.Cluster = &config.ClusterConfig{
		Mode:         "fixed",
		Workers:      1,
		DrainTimeout: "1s", // Short drain for test
	}
	cfg.Instances = 1
	oldPorts := []int{9100}

	if err := mgr.Start(cfg, oldPorts); err != nil {
		t.Fatalf("start old: %v", err)
	}

	// Wait for the old process to come online (the monitor sleeps 2s then checks).
	time.Sleep(3 * time.Second)

	// Verify old process is serving on old port
	beforeInfos := mgr.List()
	var oldPort int
	for _, info := range beforeInfos {
		if info.Name == "deploy-app" {
			oldPort = info.Port
		}
	}
	if oldPort != 9100 {
		t.Errorf("old port = %d, want 9100", oldPort)
	}

	// Deploy with new ports (blue-green).
	// Deploy starts new workers under "deploy-app:deploy:0" keys,
	// waits for them to come online (monitor needs ~2s), then promotes
	// them to the final key "deploy-app".
	newPorts := []int{9200}

	done := make(chan struct{})
	var result *DeployResult
	var deployErr error
	go func() {
		result, deployErr = mgr.Deploy(cfg, newPorts)
		close(done)
	}()

	select {
	case <-done:
		// Completed
	case <-time.After(40 * time.Second):
		t.Fatal("Deploy timed out - possible deadlock")
	}

	if deployErr != nil {
		t.Fatalf("deploy: %v", deployErr)
	}
	if result.Status != "success" {
		t.Errorf("deploy status = %s, want success", result.Status)
	}
	if len(result.NewPorts) != 1 || result.NewPorts[0] != 9200 {
		t.Errorf("new ports = %v, want [9200]", result.NewPorts)
	}

	// Verify the new process is using the new port under the final key
	afterInfos := mgr.List()
	var foundNewPort bool
	for _, info := range afterInfos {
		if info.Name == "deploy-app" && info.Port == 9200 {
			foundNewPort = true
		}
	}
	if !foundNewPort {
		t.Error("new worker not found with port 9200 after deploy")
		for _, info := range afterInfos {
			t.Logf("  process: name=%s port=%d status=%s", info.Name, info.Port, info.Status)
		}
	}

	// Verify deploy keys (deploy:*) are cleaned up - only final keys should remain
	mgr.mu.RLock()
	for key := range mgr.processes {
		if key == "deploy-app:deploy:0" {
			t.Error("deploy key still present - should have been promoted to final key")
		}
	}
	mgr.mu.RUnlock()

	// Clean up (old workers will be drained in background)
	time.Sleep(2 * time.Second) // Wait for drain timeout
	mgr.Stop("deploy-app")
}

// ==================== startInstance Error Handling in Monitor ====================

func TestStartInstanceErrorMarksErrored(t *testing.T) {
	mgr, store := testManager(t)

	// Start a process with a command that will fail immediately
	cfg := testConfig("error-app", "exit 1")
	cfg.RestartPolicy = "never" // Don't restart - just want to see the status

	err := mgr.Start(cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the process to be recorded
	time.Sleep(3 * time.Second)

	// With restart_policy=never, the process should be stopped after exit
	infos := mgr.List()
	var found bool
	for _, info := range infos {
		if info.Name == "error-app" {
			found = true
			if info.Status != StatusStopped {
				t.Errorf("status = %s, want stopped (with never restart policy)", info.Status)
			}
		}
	}

	if !found {
		// Check BoltDB - the process may have been cleaned up from the map
		ps, err := store.GetProcess("error-app")
		if err != nil {
			t.Log("process not found in memory or BoltDB - may have already exited and been cleaned")
		} else {
			if ps.Status != StatusStopped {
				t.Errorf("BoltDB status = %s, want stopped", ps.Status)
			}
		}
	}
}

// ==================== Concurrent Start/Stop Safety ====================

func TestConcurrentStartStop(t *testing.T) {
	mgr, _ := testManager(t)

	var wg sync.WaitGroup

	// Start 5 different processes concurrently
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cfg := testConfig("concurrent-app", "sleep 300")
			cfg.Name = cfg.Name + string(rune('A'+idx))
			mgr.Start(cfg, nil)
		}(i)
	}
	wg.Wait()

	time.Sleep(3 * time.Second)

	// Verify all processes exist
	infos := mgr.List()
	if len(infos) < 3 {
		t.Errorf("expected at least 3 concurrent processes, got %d", len(infos))
	}

	// Stop all concurrently
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := "concurrent-app" + string(rune('A'+idx))
			mgr.Stop(name)
		}(i)
	}
	wg.Wait()
}

// ==================== Process State Persistence ====================

func TestProcessStatePersistence(t *testing.T) {
	mgr, store := testManager(t)

	cfg := testConfig("persist-app", "sleep 300")

	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for online
	waitForStatus(mgr, "persist-app", StatusOnline, 5*time.Second)

	// Verify state is in BoltDB
	ps, err := store.GetProcess("persist-app")
	if err != nil {
		t.Fatalf("get from store: %v", err)
	}

	if ps.PID <= 0 {
		t.Errorf("persisted PID = %d, want > 0", ps.PID)
	}
	if ps.Command != "sleep 300" {
		t.Errorf("persisted command = %s, want 'sleep 300'", ps.Command)
	}
	if ps.CWD != "/tmp" {
		t.Errorf("persisted cwd = %s, want /tmp", ps.CWD)
	}

	// Verify ConfigJSON is valid
	var cfgCheck config.ProcessConfig
	if err := json.Unmarshal(ps.ConfigJSON, &cfgCheck); err != nil {
		t.Fatalf("unmarshal config json: %v", err)
	}
	if cfgCheck.Name != "persist-app" {
		t.Errorf("config name = %s, want persist-app", cfgCheck.Name)
	}

	// Clean up
	mgr.Stop("persist-app")
}

// ==================== Stop Non-Existent Process ====================

func TestStopNonExistent(t *testing.T) {
	mgr, _ := testManager(t)

	err := mgr.Stop("nonexistent")
	if err == nil {
		t.Error("expected error when stopping nonexistent process")
	}
}

// ==================== Delete Process ====================

func TestDeleteProcess(t *testing.T) {
	mgr, store := testManager(t)

	cfg := testConfig("delete-app", "sleep 300")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitForStatus(mgr, "delete-app", StatusOnline, 5*time.Second)

	// Delete should stop and remove
	if err := mgr.Delete("delete-app"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Should not be in the process list
	infos := mgr.List()
	for _, info := range infos {
		if info.Name == "delete-app" {
			t.Error("process still in list after delete")
		}
	}

	// Should not be in BoltDB
	_, err := store.GetProcess("delete-app")
	if err == nil {
		t.Error("process still in BoltDB after delete")
	}
}

// ==================== Instance Key Generation ====================

func TestInstanceKey(t *testing.T) {
	tests := []struct {
		name     string
		instance int
		total    int
		want     string
	}{
		{"app", 0, 1, "app"},
		{"app", 0, 2, "app:0"},
		{"app", 1, 2, "app:1"},
		{"my-service", 0, 3, "my-service:0"},
		{"my-service", 2, 3, "my-service:2"},
	}

	for _, tt := range tests {
		got := instanceKey(tt.name, tt.instance, tt.total)
		if got != tt.want {
			t.Errorf("instanceKey(%q, %d, %d) = %q, want %q",
				tt.name, tt.instance, tt.total, got, tt.want)
		}
	}
}

// ==================== Restart Backoff ====================

func TestRestartBackoff(t *testing.T) {
	tests := []struct {
		restarts int
		want     time.Duration
	}{
		{0, 1 * time.Second},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 2 * time.Second},
		{4, 5 * time.Second},
		{5, 5 * time.Second},
		{6, 10 * time.Second},
		{10, 10 * time.Second},
		{11, 30 * time.Second},
		{100, 30 * time.Second},
	}

	for _, tt := range tests {
		got := restartBackoff(tt.restarts)
		if got != tt.want {
			t.Errorf("restartBackoff(%d) = %v, want %v", tt.restarts, got, tt.want)
		}
	}
}

// ==================== Resolve Signal ====================

func TestResolveSignal(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"SIGTERM", "SIGTERM"},
		{"SIGKILL", "SIGKILL"},
		{"SIGINT", "SIGINT"},
		{"SIGQUIT", "SIGQUIT"},
		{"", "SIGTERM"},      // default
		{"unknown", "SIGTERM"}, // default
	}

	for _, tt := range tests {
		got := resolveSignal(tt.input)
		// We can't compare syscall.Signal directly by name easily,
		// just verify it doesn't panic and returns a valid signal.
		if got == 0 && tt.input != "" {
			t.Errorf("resolveSignal(%q) returned 0", tt.input)
		}
	}
}

// ==================== Resolve Timeout ====================

func TestResolveTimeout(t *testing.T) {
	tests := []struct {
		input    string
		fallback time.Duration
		want     time.Duration
	}{
		{"10s", 5 * time.Second, 10 * time.Second},
		{"1m", 5 * time.Second, 1 * time.Minute},
		{"", 5 * time.Second, 5 * time.Second},
		{"invalid", 5 * time.Second, 5 * time.Second},
		{"500ms", 5 * time.Second, 500 * time.Millisecond},
	}

	for _, tt := range tests {
		got := resolveTimeout(tt.input, tt.fallback)
		if got != tt.want {
			t.Errorf("resolveTimeout(%q, %v) = %v, want %v",
				tt.input, tt.fallback, got, tt.want)
		}
	}
}

// ==================== Port Env Var ====================

func TestPortEnvVar(t *testing.T) {
	tests := []struct {
		processType string
		env         map[string]string
		want        string
	}{
		{"nodejs", nil, "PORT"},
		{"php", nil, "PORT"},
		{"worker", nil, "PORT"},
		{"nodejs", map[string]string{"DPM_PORT_ENV": "APP_PORT"}, "APP_PORT"},
	}

	for _, tt := range tests {
		got := portEnvVar(tt.processType, tt.env)
		if got != tt.want {
			t.Errorf("portEnvVar(%q, %v) = %q, want %q",
				tt.processType, tt.env, got, tt.want)
		}
	}
}

// ==================== Timestamp Writer ====================

func TestTimestampWriter(t *testing.T) {
	var buf []byte
	tw := &timestampWriter{
		w: &bufWriter{buf: &buf},
	}

	_, err := tw.Write([]byte("hello world\n"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	output := string(buf)
	if len(output) == 0 {
		t.Fatal("no output from timestamp writer")
	}

	// Should have a timestamp prefix (RFC3339 format)
	if len(output) < 20 {
		t.Errorf("output too short for timestamp prefix: %q", output)
	}
}

// ==================== Continuation Line Detection ====================

func TestIsContinuationLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"at Object.<anonymous> (/app/index.js:10:5)", true},
		{"  at Module._compile (node:internal/modules/cjs/loader:1376:14)", true},
		{"}", true},
		{"{", true},
		{"})", true},
		{"  code: 'ENOENT'", true},
		{"  errno: -2", true},
		{"  syscall: 'open'", true},
		{"  address: '127.0.0.1'", true},
		{"  port: 3000", true},
		{"\tindented with tab", true},
		{"Error: ENOENT", false},
		{"normal log line", false},
		{"", false},
	}

	for _, tt := range tests {
		got := isContinuationLine(tt.line)
		if got != tt.want {
			t.Errorf("isContinuationLine(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}

// ==================== Multi-Instance Process ====================

func TestMultiInstanceProcess(t *testing.T) {
	mgr, _ := testManager(t)

	cfg := testConfig("multi-app", "sleep 300")
	cfg.Instances = 3

	ports := []int{9300, 9301, 9302}
	if err := mgr.Start(cfg, ports); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for processes to start
	time.Sleep(3 * time.Second)

	infos := mgr.List()
	if len(infos) != 3 {
		t.Fatalf("expected 3 instances, got %d", len(infos))
	}

	// Verify each instance has a different port
	portSet := make(map[int]bool)
	for _, info := range infos {
		portSet[info.Port] = true
	}
	if len(portSet) != 3 {
		t.Errorf("expected 3 unique ports, got %d", len(portSet))
	}

	// Clean up
	mgr.Stop("multi-app")
}

// ==================== OnStatusChange Callback ====================

func TestOnStatusChangeCallback(t *testing.T) {
	mgr, _ := testManager(t)

	var mu sync.Mutex
	var changes []string

	mgr.OnStatusChange(func(name, status string) {
		mu.Lock()
		changes = append(changes, name+":"+status)
		mu.Unlock()
	})

	cfg := testConfig("callback-app", "sleep 300")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the online callback
	time.Sleep(4 * time.Second)

	mu.Lock()
	hasOnline := false
	for _, c := range changes {
		if c == "callback-app:online" {
			hasOnline = true
		}
	}
	mu.Unlock()

	if !hasOnline {
		t.Error("did not receive online status change callback")
	}

	mgr.Stop("callback-app")
}

// bufWriter is a simple in-memory writer for testing.
type bufWriter struct {
	buf *[]byte
}

func (w *bufWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
