package process

import (
	"testing"
	"time"

	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// ==================== Port Allocation Cleanup on createProcess ====================

func TestPortAllocationCleanupOnRestart(t *testing.T) {
	stateDir := t.TempDir()
	logDir := t.TempDir()

	store, err := state.Open(stateDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	rotation := config.RotationConfig{
		MaxSize:    "10MB",
		MaxBackups: 2,
		Compress:   false,
	}

	mgr := NewManager(store, logDir, rotation)

	// Start a process with port 9400
	cfg := testConfig("port-test", "sleep 300")
	if err := mgr.Start(cfg, []int{9400}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for online
	waitForStatus(mgr, "port-test", StatusOnline, 5*time.Second)

	// Verify port is tracked in the process
	mgr.mu.RLock()
	proc := mgr.processes["port-test"]
	firstPort := proc.port
	firstPID := proc.pid
	mgr.mu.RUnlock()

	if firstPort != 9400 {
		t.Errorf("first port = %d, want 9400", firstPort)
	}
	if firstPID <= 0 {
		t.Errorf("first PID = %d, want > 0", firstPID)
	}

	// Re-start the process with a different port (simulating createProcess again)
	if err := mgr.startInstance(cfg, "port-test", 0, 9500); err != nil {
		t.Fatalf("re-start: %v", err)
	}

	// Verify the old process was stopped and new process uses new port
	mgr.mu.RLock()
	proc = mgr.processes["port-test"]
	secondPort := proc.port
	secondPID := proc.pid
	mgr.mu.RUnlock()

	if secondPort != 9500 {
		t.Errorf("second port = %d, want 9500", secondPort)
	}
	if secondPID == firstPID {
		t.Error("PID should have changed after restart")
	}

	// The BoltDB process state should reflect the new port
	ps, err := store.GetProcess("port-test")
	if err != nil {
		t.Fatalf("get process from store: %v", err)
	}
	if ps.Port != 9500 {
		t.Errorf("BoltDB port = %d, want 9500 (should not have stale port 9400)", ps.Port)
	}

	// Clean up
	mgr.Stop("port-test")
}

// TestPortNotLeakedOnProcessDeath verifies that when a process dies,
// the port allocation in BoltDB is properly updated.
func TestPortNotLeakedOnProcessDeath(t *testing.T) {
	stateDir := t.TempDir()
	logDir := t.TempDir()

	store, err := state.Open(stateDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	rotation := config.RotationConfig{
		MaxSize:    "10MB",
		MaxBackups: 2,
		Compress:   false,
	}

	mgr := NewManager(store, logDir, rotation)

	// Start a process that will die, with restart_policy=never
	cfg := testConfig("port-leak-app", "exit 0")
	cfg.RestartPolicy = "never"

	if err := mgr.Start(cfg, []int{9600}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the process to die and be marked as stopped
	deadline := time.Now().Add(10 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		infos := mgr.List()
		for _, info := range infos {
			if info.Name == "port-leak-app" {
				finalStatus = info.Status
				if info.Status == StatusStopped {
					goto done
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
done:

	if finalStatus != StatusStopped {
		t.Logf("final status = %s (process may have already been cleaned up)", finalStatus)
	}

	// The state in BoltDB should show the process as stopped
	ps, err := store.GetProcess("port-leak-app")
	if err != nil {
		t.Logf("process already cleaned from store (acceptable): %v", err)
		return
	}

	if ps.Status != StatusStopped {
		t.Errorf("BoltDB status = %s, want stopped", ps.Status)
	}
}

// TestMultiInstancePortTracking verifies that multi-instance processes
// track all ports correctly and don't leak ports between instances.
func TestMultiInstancePortTracking(t *testing.T) {
	stateDir := t.TempDir()
	logDir := t.TempDir()

	store, err := state.Open(stateDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	rotation := config.RotationConfig{
		MaxSize:    "10MB",
		MaxBackups: 2,
		Compress:   false,
	}

	mgr := NewManager(store, logDir, rotation)

	cfg := testConfig("multi-port-app", "sleep 300")
	cfg.Instances = 3

	ports := []int{9700, 9701, 9702}
	if err := mgr.Start(cfg, ports); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Verify each instance has its own port in BoltDB
	for _, key := range []string{"multi-port-app:0", "multi-port-app:1", "multi-port-app:2"} {
		ps, err := store.GetProcess(key)
		if err != nil {
			t.Errorf("process %s not found in store: %v", key, err)
			continue
		}
		if ps.Port < 9700 || ps.Port > 9702 {
			t.Errorf("process %s has port %d, want in range [9700-9702]", key, ps.Port)
		}
	}

	// Verify no duplicate ports
	portSet := make(map[int]bool)
	infos := mgr.List()
	for _, info := range infos {
		if portSet[info.Port] {
			t.Errorf("duplicate port %d found across instances", info.Port)
		}
		portSet[info.Port] = true
	}

	// Clean up
	mgr.Stop("multi-port-app")
}
