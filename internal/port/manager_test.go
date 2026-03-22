package port

import (
	"testing"

	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ranges := config.PortRanges{
		NodeJS:  [2]int{10000, 10099},
		Plugins: [2]int{10100, 10199},
		Workers: [2]int{10200, 10299},
	}

	return NewManager(store, ranges)
}

func TestAllocate(t *testing.T) {
	mgr := testManager(t)

	ports, err := mgr.Allocate("app-1", "nodejs", 2)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if len(ports) != 2 {
		t.Fatalf("got %d ports, want 2", len(ports))
	}
	if ports[0] < 10000 || ports[0] > 10099 {
		t.Errorf("port %d outside nodejs range", ports[0])
	}
	if ports[1] < 10000 || ports[1] > 10099 {
		t.Errorf("port %d outside nodejs range", ports[1])
	}
	if ports[0] == ports[1] {
		t.Error("allocated same port twice")
	}
}

func TestAllocateNoConflict(t *testing.T) {
	mgr := testManager(t)

	ports1, _ := mgr.Allocate("app-1", "nodejs", 1)
	ports2, _ := mgr.Allocate("app-2", "nodejs", 1)

	if ports1[0] == ports2[0] {
		t.Error("allocated conflicting ports")
	}
}

func TestAllocateSpecific(t *testing.T) {
	mgr := testManager(t)

	err := mgr.AllocateSpecific("app-1", "nodejs", 10050)
	if err != nil {
		t.Fatalf("allocate specific: %v", err)
	}

	// Same process can re-allocate same port
	err = mgr.AllocateSpecific("app-1", "nodejs", 10050)
	if err != nil {
		t.Fatalf("re-allocate same port: %v", err)
	}

	// Different process cannot take the same port
	err = mgr.AllocateSpecific("app-2", "nodejs", 10050)
	if err == nil {
		t.Error("expected error for conflicting allocation")
	}
}

func TestRelease(t *testing.T) {
	mgr := testManager(t)

	mgr.Allocate("app-1", "nodejs", 3)

	ports, _ := mgr.GetByProcess("app-1")
	if len(ports) != 3 {
		t.Fatalf("got %d ports, want 3", len(ports))
	}

	if err := mgr.Release("app-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	ports, _ = mgr.GetByProcess("app-1")
	if len(ports) != 0 {
		t.Errorf("got %d ports after release, want 0", len(ports))
	}
}

func TestReleasePort(t *testing.T) {
	mgr := testManager(t)

	ports, _ := mgr.Allocate("app-1", "nodejs", 2)

	if err := mgr.ReleasePort(ports[0]); err != nil {
		t.Fatalf("release port: %v", err)
	}

	remaining, _ := mgr.GetByProcess("app-1")
	if len(remaining) != 1 {
		t.Errorf("got %d ports, want 1", len(remaining))
	}
}

func TestAllocateTypeRanges(t *testing.T) {
	mgr := testManager(t)

	nodePorts, _ := mgr.Allocate("node-app", "nodejs", 1)
	pluginPorts, _ := mgr.Allocate("plugin", "plugins", 1)
	workerPorts, _ := mgr.Allocate("worker", "worker", 1)

	if nodePorts[0] < 10000 || nodePorts[0] > 10099 {
		t.Errorf("nodejs port %d outside range [10000-10099]", nodePorts[0])
	}
	if pluginPorts[0] < 10100 || pluginPorts[0] > 10199 {
		t.Errorf("plugins port %d outside range [10100-10199]", pluginPorts[0])
	}
	if workerPorts[0] < 10200 || workerPorts[0] > 10299 {
		t.Errorf("worker port %d outside range [10200-10299]", workerPorts[0])
	}
}

func TestAllocateExhausted(t *testing.T) {
	dir := t.TempDir()
	store, _ := state.Open(dir)
	defer store.Close()

	// Very small range: only 3 ports
	ranges := config.PortRanges{
		NodeJS: [2]int{10000, 10002},
	}
	mgr := NewManager(store, ranges)

	// Allocate all 3
	_, err := mgr.Allocate("app-1", "nodejs", 3)
	if err != nil {
		t.Fatalf("allocate 3: %v", err)
	}

	// Try to allocate one more
	_, err = mgr.Allocate("app-2", "nodejs", 1)
	if err == nil {
		t.Error("expected error when port range exhausted")
	}
}

func TestList(t *testing.T) {
	mgr := testManager(t)

	mgr.Allocate("app-1", "nodejs", 2)
	mgr.Allocate("app-2", "plugins", 1)

	all, err := mgr.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("got %d allocations, want 3", len(all))
	}
}
