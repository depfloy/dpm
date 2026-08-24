package process

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/depfloy/dpm/pkg/config"
)

// The memory recycle used to kill the only process serving a port and start the
// replacement on the freed port, which for a single-instance app behind nginx is
// a 502 for the whole boot — measured at ~52 lost requests per recycle at 200 rps
// in depfloy-app/docker/testbed/reuseport, against an app that boots in 2ms.
//
// These tests pin the orchestration: the socket-level result is what the testbed
// measures, and what matters here is that the old process is never given up
// before its replacement is actually up.

// fakeShim writes a file to stand in for the real NODE_OPTIONS shim. Nothing
// reads it in these tests — the manager only needs a non-empty path to consider
// the handover available, and the test commands are shells, not Node.
func fakeShim(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "reuseport-shim.js")
	if err := os.WriteFile(path, []byte("// test shim\n"), 0o644); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	return path
}

// recycleManager returns a manager whose watchdog fires almost immediately and
// whose handover gates are short enough for a test.
func recycleManager(t *testing.T) *Manager {
	t.Helper()
	mgr, _ := testManager(t)
	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 1
	mgr.memFloor = 1
	mgr.recycleHealthTimeout = 12 * time.Second
	mgr.recycleSettle = 300 * time.Millisecond
	mgr.SetReusePortShim(fakeShim(t))
	return mgr
}

// ==================== The old process outlives its replacement's boot ====================

func TestMemoryRecycleKeepsOldProcessUntilReplacementIsUp(t *testing.T) {
	mgr := recycleManager(t)

	// Only the process that actually grew is over its ceiling. A replacement
	// starts small, which is the whole reason replacing it helps — a stub that
	// reports every process as fat would have the replacement recycle itself the
	// moment it is promoted.
	var fatPID atomic.Int64
	mgr.readMemory = func(pid int) uint64 {
		if int64(pid) == fatPID.Load() {
			return 200 * 1024 * 1024
		}
		return 1 * 1024 * 1024
	}

	events := make(chan string, 16)
	mgr.OnRecycle(func(name, event, detail string) {
		select {
		case events <- event:
		default:
		}
	})

	cfg := memConfig("handover-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, []int{9601}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitForStatus(mgr, "handover-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("status = %q, want online", got)
	}

	old := procFor(mgr, "handover-app")
	if old == nil {
		t.Fatal("no managed process after Start")
	}
	oldPID := old.pid

	fatPID.Store(int64(oldPID))

	// Wait for the handover to finish, not for the recycle counter. The counter is
	// set when the replacement is promoted, which is before the old process has
	// been stopped — asserting on it races the second half of the handover.
	sawCompleted := false
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && !sawCompleted {
		select {
		case ev := <-events:
			if ev == "completed" {
				sawCompleted = true
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Cleanup(func() { _ = mgr.Stop("handover-app") })

	if !sawCompleted {
		t.Fatal("no completed handover event — the recycle fell back to the in-place restart")
	}

	// Exactly one instance survives, under the real key, on the same port, and it
	// is a different process than the one that breached.
	keys := keysFor(mgr, "handover-app")
	if len(keys) != 1 || keys[0] != "handover-app" {
		t.Fatalf("keys after handover = %v, want [handover-app]", keys)
	}
	now := procFor(mgr, "handover-app")
	if now == nil {
		t.Fatal("no managed process after handover")
	}
	if now.pid == oldPID {
		t.Error("process was not replaced")
	}
	if now.port != 9601 {
		t.Errorf("port = %d, want 9601 (the handover must not move the port, or nginx breaks)", now.port)
	}
	if !processAlive(now.pid) {
		t.Error("replacement is not running")
	}
	if processAlive(oldPID) {
		t.Errorf("old process (pid %d) still running after the handover completed", oldPID)
	}
	if now.memoryRecycles < 1 {
		t.Errorf("memory recycles = %d, want >= 1 carried onto the replacement", now.memoryRecycles)
	}
	if now.restarts != 0 {
		t.Errorf("restart count = %d, want 0 (a handover is not a crash)", now.restarts)
	}
}

// ==================== A replacement that will not boot costs nothing ====================

// The failure that must never happen: giving up a running site for a replacement
// that cannot serve. If the new process dies on boot, the old one keeps serving
// and stays the managed instance.
func TestMemoryRecycleAbortsWhenReplacementDies(t *testing.T) {
	mgr := recycleManager(t)
	mgr.recycleHealthTimeout = 3 * time.Second

	var over atomic.Bool
	mgr.readMemory = func(int) uint64 {
		if over.Load() {
			return 200 * 1024 * 1024
		}
		return 1 * 1024 * 1024
	}

	var aborted atomic.Int32
	mgr.OnRecycle(func(name, event, detail string) {
		if event == "aborted" {
			aborted.Add(1)
		}
	})

	cfg := memConfig("doomed-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, []int{9611}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitForStatus(mgr, "doomed-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("status = %q, want online", got)
	}
	old := procFor(mgr, "doomed-app")
	if old == nil {
		t.Fatal("no managed process after Start")
	}
	oldPID := old.pid

	// The next process this config starts exits immediately — a release that
	// crashes on boot. The running process is untouched by this.
	mgr.mu.Lock()
	cfg.Command = "exit 1"
	mgr.mu.Unlock()

	over.Store(true)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && aborted.Load() == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	over.Store(false)
	t.Cleanup(func() { _ = mgr.Stop("doomed-app") })

	if aborted.Load() == 0 {
		t.Fatal("handover never aborted; expected the replacement to fail to come up")
	}

	// The site is still up on the original process.
	if !processAlive(oldPID) {
		t.Fatalf("old process (pid %d) was killed even though the replacement never came up", oldPID)
	}
	survivor := procFor(mgr, "doomed-app")
	if survivor == nil {
		t.Fatalf("processes for doomed-app = %v, want the original still managed", keysFor(mgr, "doomed-app"))
	}
	if survivor.pid != oldPID {
		t.Errorf("managed pid = %d, want the original %d", survivor.pid, oldPID)
	}
	// No leftover recycle key.
	for _, key := range keysFor(mgr, "doomed-app") {
		if strings.HasSuffix(key, ":recycle") {
			t.Errorf("leftover replacement key %q", key)
		}
	}
}

// ==================== Handovers are serialized across the server ====================

// A handover means two copies of an application resident at once. Several at
// once multiplies that against the same RAM, and the kernel OOM killer does not
// pick the process that caused the problem — so only one runs at a time and the
// others wait for their next sample.
func TestHandoversAreSerializedPerServer(t *testing.T) {
	mgr := recycleManager(t)
	// Wide enough that two concurrent handovers would certainly overlap.
	mgr.recycleSettle = 2 * time.Second

	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 } // always over

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var started atomic.Int32
	mgr.OnRecycle(func(name, event, detail string) {
		switch event {
		case "starting":
			started.Add(1)
			cur := inFlight.Add(1)
			for {
				peak := maxInFlight.Load()
				if cur <= peak || maxInFlight.CompareAndSwap(peak, cur) {
					break
				}
			}
		case "completed", "aborted", "unavailable":
			inFlight.Add(-1)
		}
	})

	for i, name := range []string{"multi-a", "multi-b", "multi-c"} {
		cfg := memConfig(name, "sleep 300", "100MB")
		if err := mgr.Start(cfg, []int{9621 + i}); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(func() { _ = mgr.Stop(name) })
	}
	// Deliberately no wait for all three to be online at once. Every one of them
	// is permanently over its ceiling here, so they are continuously being handed
	// over and there is no moment when all three are settled — requiring one made
	// this test fail on a loaded machine for a reason unrelated to what it checks.
	// Soak instead, and assert the only thing that matters: the overlaps never
	// stack.
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if maxInFlight.Load() > 1 {
			break // already violated; no reason to keep waiting
		}
		time.Sleep(100 * time.Millisecond)
	}

	if peak := maxInFlight.Load(); peak > 1 {
		t.Errorf("concurrent handovers = %d, want at most 1 (each one doubles an app's RSS)", peak)
	}
	if started.Load() == 0 {
		t.Error("no handover was attempted; the test proved nothing")
	}
}

// ==================== The shim reaches Node without clobbering the customer ====================

func TestReusePortShimEnvInjection(t *testing.T) {
	shim := "/usr/local/lib/dpm/reuseport-shim.js"

	cases := []struct {
		name string
		typ  string
		env  []string
		want string
	}{
		{
			name: "added when absent",
			typ:  "nodejs",
			env:  []string{"PATH=/usr/bin"},
			want: "NODE_OPTIONS=--require " + shim,
		},
		{
			name: "appended to the customer's own flags",
			typ:  "nodejs",
			env:  []string{"NODE_OPTIONS=--max-old-space-size=512"},
			want: "NODE_OPTIONS=--max-old-space-size=512 --require " + shim,
		},
		{
			name: "fills an empty value",
			typ:  "nodejs",
			env:  []string{"NODE_OPTIONS="},
			want: "NODE_OPTIONS=--require " + shim,
		},
		{
			name: "not stacked when already present",
			typ:  "nodejs",
			env:  []string{"NODE_OPTIONS=--require " + shim},
			want: "NODE_OPTIONS=--require " + shim,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, enabled := withReusePortShim(append([]string{}, tc.env...), &config.ProcessConfig{Type: tc.typ}, shim)
			if !enabled {
				t.Fatal("shim reported as not enabled for a nodejs process")
			}
			var found string
			for _, e := range got {
				if strings.HasPrefix(e, "NODE_OPTIONS=") {
					found = e
				}
			}
			if found != tc.want {
				t.Errorf("NODE_OPTIONS = %q, want %q", found, tc.want)
			}
		})
	}

	t.Run("left alone for non-node processes", func(t *testing.T) {
		got, enabled := withReusePortShim([]string{"PATH=/usr/bin"}, &config.ProcessConfig{Type: "worker"}, shim)
		if enabled {
			t.Error("a non-node process must not be reported as reusePort-capable")
		}
		for _, e := range got {
			if strings.HasPrefix(e, "NODE_OPTIONS=") {
				t.Errorf("NODE_OPTIONS injected into a %q process: %q", "worker", e)
			}
		}
	})

	t.Run("no shim configured is a no-op", func(t *testing.T) {
		got, enabled := withReusePortShim([]string{"PATH=/usr/bin"}, &config.ProcessConfig{Type: "nodejs"}, "")
		if enabled {
			t.Error("no shim configured must not report reusePort capability")
		}
		if len(got) != 1 {
			t.Errorf("env = %v, want unchanged", got)
		}
	})
}

// ==================== Without a shim the old behaviour is unchanged ====================

// A server whose processes were started before the shim existed cannot hand over:
// their sockets do not carry SO_REUSEPORT, so nothing can bind beside them. Those
// recycles must still happen, the old way, rather than silently stopping.
func TestRecycleFallsBackWhenNoShimConfigured(t *testing.T) {
	mgr, _ := testManager(t)
	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 1
	mgr.memFloor = 1
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 }
	// Deliberately no SetReusePortShim.

	cfg := memConfig("legacy-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, []int{9631}); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Stop("legacy-app") })

	if got := waitForRecycles(mgr, "legacy-app", 2, 25*time.Second); got < 2 {
		t.Errorf("memory recycles = %d, want >= 2 via the in-place path", got)
	}
	if keys := keysFor(mgr, "legacy-app"); len(keys) != 1 || keys[0] != "legacy-app" {
		t.Errorf("keys = %v, want [legacy-app]", keys)
	}
}

// ==================== A process that predates the shim still gets recycled ====================

// Upgrading the daemon does not restart the processes it adopts, so right after
// an upgrade every running application bound its port without SO_REUSEPORT.
// Nothing can bind beside those. Attempting the handover anyway would abort every
// time, and because an aborted handover deliberately does not fall back, they
// would never be recycled at all — growing until the kernel picked a victim.
//
// So the capability is a property of the process, not of the daemon's config: a
// process that cannot share its port is replaced in place, as before, until its
// next deployment starts it with the shim.
func TestProcessWithoutShimStillRecyclesInPlace(t *testing.T) {
	mgr := recycleManager(t) // shim IS configured on the manager
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 }

	var handovers atomic.Int32
	mgr.OnRecycle(func(name, event, detail string) {
		if event == "starting" {
			handovers.Add(1)
		}
	})

	cfg := memConfig("adopted-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, []int{9641}); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Stop("adopted-app") })
	if got := waitForStatus(mgr, "adopted-app", StatusOnline, 15*time.Second); got != StatusOnline {
		t.Fatalf("status = %q, want online", got)
	}

	// Model the adopted process: running, managed, but its socket does not carry
	// SO_REUSEPORT because it was started before the shim existed.
	// startInstance keeps setting this on each replacement, so clear it on every
	// instance of this app for the rest of the run.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			mgr.mu.Lock()
			for _, proc := range mgr.processes {
				if proc.config != nil && proc.config.Name == "adopted-app" {
					proc.hasReusePort = false
				}
			}
			mgr.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// The watchdog may already have fired once before the flag was cleared; only
	// what happens from here on is what this test is about.
	time.Sleep(200 * time.Millisecond)
	handovers.Store(0)
	baseline := 0
	for _, info := range mgr.List() {
		if info.Name == "adopted-app" {
			baseline = info.MemoryRecycles
		}
	}

	if got := waitForRecycles(mgr, "adopted-app", baseline+2, 30*time.Second); got < baseline+2 {
		t.Errorf("memory recycles = %d, want >= 2 — a process that cannot share its port must still be recycled", got)
	}
	if n := handovers.Load(); n != 0 {
		t.Errorf("handovers attempted = %d, want 0 for a process without the shim", n)
	}
}
