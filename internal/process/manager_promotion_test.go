package process

import (
	"sync/atomic"
	"testing"
	"time"
)

// Regression tests for the commerce-v2 outage of 2026-08-24.
//
// Nine React Router storefronts share one box. Three of them showed up twice in
// `dpm list` — one live entry and one dead entry stuck in "starting" on the same
// port — and /var/log/dpm/daemon.log recorded memory recycles for processes that
// had been promoted and serving for weeks under names like "app_244:deploy:0".
// Then a routine deploy of one of those projects took the site down for good:
//
//	blue-green deploy completed  name=app_243 new_ports=[3004] old_ports=[3180,3180]
//	old workers drained          name=app_243
//	process unhealthy            name=app_243 dial tcp 127.0.0.1:3004: connection refused
//
// The deployment was recorded successful. The site stayed down until a human
// redeployed it.
//
// Two defects, one behind the other. These tests pin each one separately so a
// fix to the second cannot hide a regression in the first.

// keysFor returns the map keys currently holding a process for this config name.
func keysFor(m *Manager, name string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for key, proc := range m.processes {
		if proc.config.Name == name {
			keys = append(keys, key)
		}
	}
	return keys
}

// procFor returns the single managed process for name, or nil if there is not
// exactly one.
func procFor(m *Manager, name string) *managed {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var found *managed
	for _, proc := range m.processes {
		if proc.config.Name != name {
			continue
		}
		if found != nil {
			return nil // more than one: caller asserts on keysFor
		}
		found = proc
	}
	return found
}

func pendingDrainHas(m *Manager, name string, target *managed) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, proc := range m.pendingDrain[name] {
		if proc == target {
			return true
		}
	}
	return false
}

// waitForRecycles polls until the named process reports at least want recycles.
func waitForRecycles(m *Manager, name string, want int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	best := 0
	for time.Now().Before(deadline) {
		for _, info := range m.List() {
			if info.Name == name && info.MemoryRecycles > best {
				best = info.MemoryRecycles
			}
		}
		if best >= want {
			return best
		}
		time.Sleep(25 * time.Millisecond)
	}
	return best
}

// ==================== Defect 1: the promotion never reaches the monitors ====================

// Deploy promotes a worker by moving the map entry from "app:deploy:0" to "app"
// (manager.go step 4). But startInstance handed that key to two long-lived
// goroutines by value:
//
//	go m.monitor(proc, key, logFile, errFile)
//	go m.monitorMemory(proc, key, limit)
//
// Moving the map entry cannot rename a string already captured in a running
// closure, so both monitors keep saying "app:deploy:0" for the rest of the
// process's life. The first memory recycle then re-registers the live process
// under that stale key and strands the promoted entry — dead, frozen at
// "starting" — under the real one. That is the duplicate seen in production, and
// it is what arms the second defect below.
func TestPromotedWorkerKeepsFinalKeyAfterMemoryRecycle(t *testing.T) {
	mgr, _ := testManager(t)

	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 1
	mgr.memFloor = 1

	// Over the ceiling only while we ask for it, so the process recycles once
	// and then settles instead of flapping through the assertions.
	var over atomic.Bool
	mgr.readMemory = func(int) uint64 {
		if over.Load() {
			return 200 * 1024 * 1024
		}
		return 1 * 1024 * 1024
	}

	cfg := memConfig("promoted-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, []int{9301}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitForStatus(mgr, "promoted-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("pre-deploy status = %q, want online", got)
	}

	// Promote a new worker the way a real deployment does.
	if _, err := mgr.Deploy(cfg, []int{9302}); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if err := mgr.Drain("promoted-app"); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// The promoted worker is serving. Push it over its ceiling once.
	over.Store(true)
	if got := waitForRecycles(mgr, "promoted-app", 1, 15*time.Second); got < 1 {
		t.Fatalf("memory recycles = %d, want >= 1 (watchdog never fired)", got)
	}
	over.Store(false)
	if got := waitForStatus(mgr, "promoted-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("post-recycle status = %q, want online", got)
	}
	t.Cleanup(func() { _ = mgr.Stop("promoted-app") })

	// A recycle replaces a process; it must not multiply the entries tracking it
	// or move the survivor onto a deploy key.
	keys := keysFor(mgr, "promoted-app")
	if len(keys) != 1 {
		t.Fatalf("keys after recycle = %v, want exactly one (the promoted worker was stranded)", keys)
	}
	if keys[0] != "promoted-app" {
		t.Errorf("surviving key = %q, want %q (recycle re-registered under the stale deploy key)",
			keys[0], "promoted-app")
	}
}

// ==================== Defect 2: the deploy drains the worker it just promoted ====================

// This is the outage. Once a live process sits on a deploy key, Deploy eats it:
//
//	step 1 collects old workers by config.Name, so "app:deploy:0" lands in oldKeys
//	step 2 calls startInstance with that same key, whose prologue kills whatever
//	       is there — the process currently serving traffic, while nginx still
//	       points at its port
//	step 4 collects oldWorkers from oldKeys *before* promoting, finds the brand
//	       new process under "app:deploy:0", and deletes it from the map; the
//	       promotion loop then looks for a key that is already gone and silently
//	       does nothing
//	step 5 parks that new process in pendingDrain
//	Drain  kills it, after Depfloy has already pointed nginx at its port
//
// Result: no listener on the new port, no entry in the manager at all, and a
// deployment recorded as successful.
//
// The drifted state is built directly here rather than via a recycle, so this
// test keeps failing for the right reason even after defect 1 is fixed: no
// matter how a deploy key comes to hold a live process, a deploy must never
// drain the worker it just promoted.
func TestDeployDoesNotDrainThePromotedWorker(t *testing.T) {
	mgr, _ := testManager(t)

	cfg := testConfig("drift-app", "sleep 300")
	if err := mgr.Start(cfg, []int{9401}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitForStatus(mgr, "drift-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("pre-deploy status = %q, want online", got)
	}

	// Reproduce the drift: the serving process is registered under the deploy key
	// that the next deploy is about to claim. proc.key moves with it, which is the
	// state a daemon restart leaves behind — adoptOrphans re-attaches the process
	// under whatever key the store recorded, so upgrading the binary does not heal
	// a server that is already drifted.
	mgr.mu.Lock()
	live := mgr.processes["drift-app"]
	if live == nil {
		mgr.mu.Unlock()
		t.Fatal("no process under the final key after Start")
	}
	delete(mgr.processes, "drift-app")
	live.key = "drift-app:deploy:0"
	mgr.processes["drift-app:deploy:0"] = live
	mgr.mu.Unlock()

	if _, err := mgr.Deploy(cfg, []int{9402}); err != nil {
		t.Fatalf("deploy: %v", err)
	}

	// Zero-downtime, the actual promise: Depfloy has not switched nginx yet, so
	// the old worker must still be serving its port when Deploy returns. Starting
	// the replacement on a key the live process occupies used to kill it here,
	// leaving the old port dark for the whole health-check and soak window.
	if !processAlive(live.pid) {
		t.Errorf("old worker (pid %d, port %d) died during the deploy — nginx still points at it: 502",
			live.pid, live.port)
	}

	// Whatever Deploy promoted must still be managed, and must not be queued for
	// the drain that follows the nginx switch.
	promoted := procFor(mgr, "drift-app")
	if promoted == nil {
		t.Fatalf("processes for drift-app after deploy = %v, want exactly one promoted worker",
			keysFor(mgr, "drift-app"))
	}
	if promoted.port != 9402 {
		t.Errorf("promoted port = %d, want 9402", promoted.port)
	}
	if pendingDrainHas(mgr, "drift-app", promoted) {
		t.Error("the promoted worker is queued in pendingDrain — Drain will kill the site")
	}

	// Depfloy switches nginx to the new port and then drains. The promoted worker
	// has to survive that.
	if err := mgr.Drain("drift-app"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Stop("drift-app") })

	if !processAlive(promoted.pid) {
		t.Fatalf("promoted worker (pid %d, port %d) was killed by the post-switch drain: permanent 502",
			promoted.pid, promoted.port)
	}
	if got := waitForStatus(mgr, "drift-app", StatusOnline, 10*time.Second); got != StatusOnline {
		t.Errorf("status after drain = %q, want online", got)
	}
	if keys := keysFor(mgr, "drift-app"); len(keys) != 1 || keys[0] != "drift-app" {
		t.Errorf("keys after deploy+drain = %v, want [drift-app]", keys)
	}
}

// ==================== Drain refuses to kill a live instance ====================

// Deploy is supposed to park only retired workers, and after the two fixes above
// it does. This is the layer under that: Depfloy calls Drain *after* it has
// pointed nginx at the new port, so a process wrongly parked here is an outage
// with no way back — the site stays down until a human redeploys, and the
// deployment was already recorded successful. Drain therefore refuses to kill
// anything that is still the managed instance, which turns a future regression in
// Deploy into a leaked entry rather than a dead site.
func TestDrainRefusesToKillTheManagedInstance(t *testing.T) {
	mgr, _ := testManager(t)

	cfg := testConfig("guard-app", "sleep 300")
	if err := mgr.Start(cfg, []int{9501}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitForStatus(mgr, "guard-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("status = %q, want online", got)
	}
	t.Cleanup(func() { _ = mgr.Stop("guard-app") })

	live := procFor(mgr, "guard-app")
	if live == nil {
		t.Fatal("no managed process after Start")
	}

	// Simulate the regression: the live instance ends up queued for the drain.
	mgr.mu.Lock()
	mgr.pendingDrain["guard-app"] = append(mgr.pendingDrain["guard-app"], live)
	mgr.mu.Unlock()

	if err := mgr.Drain("guard-app"); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if !processAlive(live.pid) {
		t.Fatalf("Drain killed the managed instance (pid %d, port %d): permanent 502",
			live.pid, live.port)
	}
	if got := waitForStatus(mgr, "guard-app", StatusOnline, 10*time.Second); got != StatusOnline {
		t.Errorf("status after drain = %q, want online", got)
	}
}
