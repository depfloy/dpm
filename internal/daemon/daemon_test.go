package daemon

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/depfloy/dpm/internal/port"
	"github.com/depfloy/dpm/internal/process"
	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// testDaemonComponents creates the subsystems needed for testing daemon
// behavior without starting the full daemon (no socket, no signal handler).
func testDaemonComponents(t *testing.T) (*state.Store, *process.Manager, *port.Manager) {
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

	pm := process.NewManager(store, logDir, rotation)

	portMgr := port.NewManager(store)

	return store, pm, portMgr
}

// TestAdoptOrphansRestartsAllWorkerPorts verifies that when a multi-worker
// process is found dead on startup, adoptOrphans restarts ALL of its workers
// (one per saved instance port), not collapsing it down to a single worker.
// This exercises the real d.adoptOrphans() against the per-base-name port
// accumulation fix.
func TestAdoptOrphansRestartsAllWorkerPorts(t *testing.T) {
	store, pm, portMgr := testDaemonComponents(t)

	d := &Daemon{
		store:          store,
		processManager: pm,
		portManager:    portMgr,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:          "multi-app",
		Command:       "sleep 300",
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     2,
		RestartPolicy: "always",
	})

	// Two dead instances of the same base process, each with its own port.
	const deadPID = 999999999
	for i, portNum := range []int{12001, 12002} {
		key := "multi-app:" + string(rune('0'+i))
		store.SaveProcess(&state.ProcessState{
			Name:       key,
			PID:        deadPID,
			Port:       portNum,
			Status:     "online",
			Command:    "sleep 300",
			CWD:        "/tmp",
			Type:       "nodejs",
			ConfigJSON: cfgJSON,
		})
	}

	if err := d.adoptOrphans(); err != nil {
		t.Fatalf("adoptOrphans: %v", err)
	}

	// Give the restarted workers a moment to register.
	time.Sleep(1 * time.Second)

	ports := map[int]bool{}
	count := 0
	for _, info := range pm.List() {
		if info.Type == "nodejs" && info.Command == "sleep 300" {
			count++
			ports[info.Port] = true
		}
	}
	if count != 2 {
		t.Errorf("restarted worker count = %d, want 2 (multi-worker app must not collapse to one)", count)
	}
	if !ports[12001] || !ports[12002] {
		t.Errorf("restarted ports = %v, want both 12001 and 12002", ports)
	}

	// Clean up the spawned processes.
	pm.Stop("multi-app")
}

// ==================== adoptOrphans Releases Ports for Dead Processes ====================

func TestAdoptOrphansReleasesPortsForDeadProcesses(t *testing.T) {
	store, _, portMgr := testDaemonComponents(t)

	// Simulate a process state saved in BoltDB from a previous daemon instance.
	// Use a PID that is definitely dead (very high PID unlikely to exist).
	deadPID := 999999999

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:          "dead-app",
		Command:       "sleep 300",
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     1,
		RestartPolicy: "always",
	})

	// Save process state with port allocation
	store.SaveProcess(&state.ProcessState{
		Name:       "dead-app",
		PID:        deadPID,
		Port:       11050,
		Status:     "online",
		Command:    "sleep 300",
		CWD:        "/tmp",
		Type:       "nodejs",
		ConfigJSON: cfgJSON,
	})

	// Also save a port allocation
	store.SavePortAllocation(&state.PortAllocation{
		Port:        11050,
		ProcessName: "dead-app",
		Type:        "nodejs",
		AllocatedAt: time.Now(),
	})

	// Verify the port is allocated before adoption
	_, err := store.GetPortAllocation(11050)
	if err != nil {
		t.Fatalf("port should be allocated before adoption: %v", err)
	}

	// Run adoptOrphans logic manually (extracted from daemon.go).
	// Since the PID is dead, it should clean up both the process and port.
	processes, err := store.ListProcesses()
	if err != nil {
		t.Fatalf("list processes: %v", err)
	}

	for _, ps := range processes {
		if ps.PID <= 0 || !processAlive(ps.PID) {
			// Dead process - clean up
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
		}
	}

	// Verify process is removed from store
	_, err = store.GetProcess("dead-app")
	if err == nil {
		t.Error("dead process should have been removed from store")
	}

	// Verify port allocation is cleaned up
	_, err = store.GetPortAllocation(11050)
	if err == nil {
		t.Error("port allocation should have been cleaned up for dead process")
	}
}

// TestAdoptOrphansKeepsAliveProcesses verifies that adoptOrphans
// does NOT clean up processes that are still running.
func TestAdoptOrphansKeepsAliveProcesses(t *testing.T) {
	store, pm, _ := testDaemonComponents(t)

	// Start a real process that will be alive
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	alivePID := cmd.Process.Pid

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:          "alive-app",
		Command:       "sleep 300",
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     1,
		RestartPolicy: "always",
	})

	// Save process state
	store.SaveProcess(&state.ProcessState{
		Name:         "alive-app",
		PID:          alivePID,
		Port:         11060,
		Status:       "online",
		Command:      "sleep 300",
		CWD:          "/tmp",
		Type:         "nodejs",
		RestartCount: 0,
		StartedAt:    time.Now(),
		ConfigJSON:   cfgJSON,
	})

	// Attempt to adopt - should succeed because the PID is alive
	ps, err := store.GetProcess("alive-app")
	if err != nil {
		t.Fatalf("get process: %v", err)
	}

	err = pm.Attach(ps)
	if err != nil {
		t.Fatalf("attach alive process: %v", err)
	}

	// Process should be in the manager's list
	infos := pm.List()
	var found bool
	for _, info := range infos {
		if info.Name == "alive-app" {
			found = true
			if info.PID != alivePID {
				t.Errorf("adopted PID = %d, want %d", info.PID, alivePID)
			}
			if info.Status != "online" {
				t.Errorf("adopted status = %s, want online", info.Status)
			}
		}
	}
	if !found {
		t.Error("alive process was not adopted into manager")
	}
}

// TestAdoptOrphansCleansErroredProcesses verifies that processes in
// errored state are cleaned up during adoption (not re-adopted).
func TestAdoptOrphansCleansErroredProcesses(t *testing.T) {
	store, _, portMgr := testDaemonComponents(t)

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:    "errored-app",
		Command: "sleep 300",
		CWD:     "/tmp",
		Type:    "nodejs",
	})

	// Save an errored process
	store.SaveProcess(&state.ProcessState{
		Name:         "errored-app",
		PID:          12345, // PID doesn't matter since status check comes first
		Port:         11070,
		Status:       "errored",
		RestartCount: 50,
		ConfigJSON:   cfgJSON,
	})

	store.SavePortAllocation(&state.PortAllocation{
		Port:        11070,
		ProcessName: "errored-app",
		Type:        "nodejs",
		AllocatedAt: time.Now(),
	})

	// Run adoptOrphans logic: errored processes should be cleaned
	processes, _ := store.ListProcesses()
	for _, ps := range processes {
		if ps.RestartCount >= 50 || ps.Status == "errored" || ps.Status == "stopped" {
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
		}
	}

	// Verify cleanup
	_, err := store.GetProcess("errored-app")
	if err == nil {
		t.Error("errored process should have been cleaned up")
	}

	_, err = store.GetPortAllocation(11070)
	if err == nil {
		t.Error("port for errored process should have been released")
	}
}

// TestAdoptOrphansHandlesNoPIDProcess verifies that processes saved
// without a valid PID (PID <= 0) are cleaned up.
func TestAdoptOrphansHandlesNoPIDProcess(t *testing.T) {
	store, _, portMgr := testDaemonComponents(t)

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:    "no-pid-app",
		Command: "sleep 300",
		CWD:     "/tmp",
		Type:    "nodejs",
	})

	// Save a process with PID=0 (e.g., from a failed start)
	store.SaveProcess(&state.ProcessState{
		Name:       "no-pid-app",
		PID:        0,
		Port:       11080,
		Status:     "starting",
		ConfigJSON: cfgJSON,
	})

	store.SavePortAllocation(&state.PortAllocation{
		Port:        11080,
		ProcessName: "no-pid-app",
		Type:        "nodejs",
		AllocatedAt: time.Now(),
	})

	// Run adoptOrphans logic: PID <= 0 should be cleaned
	processes, _ := store.ListProcesses()
	for _, ps := range processes {
		if ps.PID <= 0 {
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
		}
	}

	_, err := store.GetProcess("no-pid-app")
	if err == nil {
		t.Error("process with no PID should have been cleaned up")
	}

	_, err = store.GetPortAllocation(11080)
	if err == nil {
		t.Error("port for no-PID process should have been released")
	}
}

// TestAdoptOrphansMultipleProcessMix tests the scenario where BoltDB
// contains a mix of alive, dead, errored, and no-PID processes.
func TestAdoptOrphansMultipleProcessMix(t *testing.T) {
	store, pm, portMgr := testDaemonComponents(t)

	// 1. Alive process
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:          "alive-mix",
		Command:       "sleep 300",
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     1,
		RestartPolicy: "always",
	})

	store.SaveProcess(&state.ProcessState{
		Name:       "alive-mix",
		PID:        cmd.Process.Pid,
		Port:       11090,
		Status:     "online",
		StartedAt:  time.Now(),
		ConfigJSON: cfgJSON,
	})

	// 2. Dead process
	store.SaveProcess(&state.ProcessState{
		Name:       "dead-mix",
		PID:        999999998,
		Port:       11091,
		Status:     "online",
		ConfigJSON: cfgJSON, // reuse config
	})
	store.SavePortAllocation(&state.PortAllocation{
		Port:        11091,
		ProcessName: "dead-mix",
		Type:        "nodejs",
	})

	// 3. Errored process
	store.SaveProcess(&state.ProcessState{
		Name:         "errored-mix",
		PID:          999999997,
		Port:         11092,
		Status:       "errored",
		RestartCount: 50,
		ConfigJSON:   cfgJSON,
	})
	store.SavePortAllocation(&state.PortAllocation{
		Port:        11092,
		ProcessName: "errored-mix",
		Type:        "nodejs",
	})

	// Run full adoptOrphans logic
	processes, _ := store.ListProcesses()
	adopted := 0
	cleaned := 0

	for _, ps := range processes {
		if ps.RestartCount >= 50 || ps.Status == "errored" || ps.Status == "stopped" {
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
			cleaned++
			continue
		}
		if ps.PID <= 0 {
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
			cleaned++
			continue
		}
		if processAlive(ps.PID) {
			if err := pm.Attach(ps); err != nil {
				store.DeleteProcess(ps.Name)
				if ps.Port > 0 {
					portMgr.ReleasePort(ps.Port)
				}
				cleaned++
				continue
			}
			adopted++
		} else {
			store.DeleteProcess(ps.Name)
			if ps.Port > 0 {
				portMgr.ReleasePort(ps.Port)
			}
			cleaned++
		}
	}

	if adopted != 1 {
		t.Errorf("adopted = %d, want 1 (only alive process)", adopted)
	}
	if cleaned != 2 {
		t.Errorf("cleaned = %d, want 2 (dead + errored)", cleaned)
	}

	// Verify alive process is in manager
	infos := pm.List()
	var foundAlive bool
	for _, info := range infos {
		if info.Name == "alive-mix" {
			foundAlive = true
		}
	}
	if !foundAlive {
		t.Error("alive process should be in manager after adoption")
	}

	// Verify dead process port is released
	_, err := store.GetPortAllocation(11091)
	if err == nil {
		t.Error("dead process port should have been released")
	}

	// Verify errored process port is released
	_, err = store.GetPortAllocation(11092)
	if err == nil {
		t.Error("errored process port should have been released")
	}
}

// TestProcessAlive verifies the processAlive utility function.
func TestProcessAlive(t *testing.T) {
	// Current process should be alive
	if !processAlive(os.Getpid()) {
		t.Error("current process should be alive")
	}

	// PID 0 should not be considered alive
	if processAlive(0) {
		t.Error("PID 0 should not be alive")
	}

	// Negative PID should not be alive
	if processAlive(-1) {
		t.Error("negative PID should not be alive")
	}

	// Very high PID should not be alive
	if processAlive(999999999) {
		t.Error("PID 999999999 should not be alive")
	}
}

// TestAdoptOrphansPreservesRestartCount verifies that when a process
// is adopted, its restart count from BoltDB is preserved.
func TestAdoptOrphansPreservesRestartCount(t *testing.T) {
	store, pm, _ := testDaemonComponents(t)

	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	cfgJSON, _ := json.Marshal(&config.ProcessConfig{
		Name:          "restart-count-app",
		Command:       "sleep 300",
		CWD:           "/tmp",
		Type:          "nodejs",
		Instances:     1,
		RestartPolicy: "always",
	})

	// Save with restart count = 7 (simulating previous crashes)
	store.SaveProcess(&state.ProcessState{
		Name:         "restart-count-app",
		PID:          cmd.Process.Pid,
		Port:         0,
		Status:       "online",
		RestartCount: 7,
		StartedAt:    time.Now(),
		ConfigJSON:   cfgJSON,
	})

	ps, _ := store.GetProcess("restart-count-app")
	if err := pm.Attach(ps); err != nil {
		t.Fatalf("attach: %v", err)
	}

	infos := pm.List()
	for _, info := range infos {
		if info.Name == "restart-count-app" {
			if info.RestartCount != 7 {
				t.Errorf("restart count = %d, want 7 (preserved from BoltDB)", info.RestartCount)
			}
			return
		}
	}
	t.Error("adopted process not found in list")
}
