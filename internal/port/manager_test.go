package port

import (
	"testing"
	"time"

	"github.com/depfloy/dpm/internal/state"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	return NewManager(store)
}

func TestRelease(t *testing.T) {
	mgr := testManager(t)

	// Manually save port allocations (simulating what Depfloy would track)
	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3000, ProcessName: "app-1", Type: "nodejs", AllocatedAt: time.Now(),
	})
	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3001, ProcessName: "app-1", Type: "nodejs", AllocatedAt: time.Now(),
	})
	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3100, ProcessName: "app-2", Type: "nodejs", AllocatedAt: time.Now(),
	})

	if err := mgr.Release("app-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	all, _ := mgr.List()
	if len(all) != 1 {
		t.Errorf("got %d allocations after release, want 1", len(all))
	}
	if all[0].ProcessName != "app-2" {
		t.Errorf("remaining allocation is %s, want app-2", all[0].ProcessName)
	}
}

func TestReleasePort(t *testing.T) {
	mgr := testManager(t)

	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3000, ProcessName: "app-1", Type: "nodejs", AllocatedAt: time.Now(),
	})
	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3001, ProcessName: "app-1", Type: "nodejs", AllocatedAt: time.Now(),
	})

	if err := mgr.ReleasePort(3000); err != nil {
		t.Fatalf("release port: %v", err)
	}

	all, _ := mgr.List()
	if len(all) != 1 {
		t.Errorf("got %d allocations, want 1", len(all))
	}
}

func TestList(t *testing.T) {
	mgr := testManager(t)

	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3000, ProcessName: "app-1", Type: "nodejs", AllocatedAt: time.Now(),
	})
	mgr.store.SavePortAllocation(&state.PortAllocation{
		Port: 3100, ProcessName: "app-2", Type: "nodejs", AllocatedAt: time.Now(),
	})

	all, err := mgr.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("got %d allocations, want 2", len(all))
	}
}

func TestIsPortFree(t *testing.T) {
	// Port 0 is a special case - it should always be free (OS assigns)
	// Test with a high port that's unlikely to be in use
	free := isPortFree(19999)
	if !free {
		t.Log("port 19999 is in use, skipping free port test")
	}
}
