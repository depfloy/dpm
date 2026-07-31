package process

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/depfloy/dpm/pkg/config"
)

// ==================== parseMemoryLimit ====================

func TestParseMemoryLimit(t *testing.T) {
	const floor = 32 * 1024 * 1024

	cfgWith := func(v string) *config.ProcessConfig {
		return &config.ProcessConfig{Resources: &config.ResourceLimits{MaxMemory: v}}
	}

	cases := []struct {
		name string
		cfg  *config.ProcessConfig
		want uint64
	}{
		{"megabytes", cfgWith("512MB"), 512 * 1024 * 1024},
		{"gigabytes", cfgWith("1GB"), 1024 * 1024 * 1024},
		{"kilobytes rounds below floor to disabled", cfgWith("1024KB"), 0},
		{"bare bytes below floor disabled", cfgWith("1000B"), 0},
		{"lowercase suffix", cfgWith("128mb"), 128 * 1024 * 1024},
		{"whitespace", cfgWith("  256MB  "), 256 * 1024 * 1024},
		{"empty is disabled", cfgWith(""), 0},
		{"garbage is disabled", cfgWith("abc"), 0},
		{"zero is disabled", cfgWith("0"), 0},
		{"below floor is disabled", cfgWith("10MB"), 0},
		{"nil resources is disabled", &config.ProcessConfig{}, 0},
		{"nil config is disabled", nil, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseMemoryLimit(tc.cfg, floor); got != tc.want {
				t.Errorf("parseMemoryLimit(%v) = %d, want %d", tc.cfg, got, tc.want)
			}
		})
	}
}

// memConfig is testConfig plus a real memory limit.
func memConfig(name, command, maxMemory string) *config.ProcessConfig {
	cfg := testConfig(name, command)
	cfg.Resources = &config.ResourceLimits{MaxMemory: maxMemory}
	return cfg
}

// ==================== Memory limit triggers restart ====================

func TestMemoryLimitTriggersRestart(t *testing.T) {
	mgr, _ := testManager(t)

	// Fast, deterministic enforcement: no warm-up, tight interval, low breach
	// count, floor out of the way. readMemory is stubbed always over the limit.
	mgr.memWarmup = 0
	mgr.memInterval = 50 * time.Millisecond
	mgr.memBreaches = 2
	mgr.memFloor = 1
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 }

	var fired int32
	mgr.OnMemoryLimit(func(string, uint64, uint64) { atomic.AddInt32(&fired, 1) })

	cfg := memConfig("mem-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A live process kept over its limit must be replaced repeatedly.
	deadline := time.Now().Add(15 * time.Second)
	var recycles, restarts int
	for time.Now().Before(deadline) {
		for _, info := range mgr.List() {
			if info.Name != "mem-app" {
				continue
			}
			if info.MemoryRecycles > recycles {
				recycles = info.MemoryRecycles
			}
			if info.RestartCount > restarts {
				restarts = info.RestartCount
			}
		}
		if recycles >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = mgr.Stop("mem-app")

	if recycles < 2 {
		t.Errorf("memory recycles = %d, want >= 2 (memory limit should drive replacements)", recycles)
	}
	// The recycle is a replacement we asked for, so it must not spend the crash
	// budget. When it did, an application that only ever needed more memory than
	// its ceiling walked to maxRestarts and was stopped for good.
	if restarts != 0 {
		t.Errorf("restart count = %d, want 0 (a memory recycle is not a crash)", restarts)
	}
	if atomic.LoadInt32(&fired) == 0 {
		t.Error("onMemoryLimit callback never fired")
	}
}

// ==================== Hysteresis: a transient spike must not restart ====================

func TestMemoryHysteresisNoRestartOnSpike(t *testing.T) {
	mgr, _ := testManager(t)

	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 3
	mgr.memFloor = 1

	// Alternate over/under so the consecutive-breach counter resets every other
	// sample and never reaches the threshold. Atomic: the sampler goroutine reads
	// it concurrently.
	var n int64
	mgr.readMemory = func(int) uint64 {
		if atomic.AddInt64(&n, 1)%2 == 0 {
			return 200 * 1024 * 1024 // over
		}
		return 50 * 1024 * 1024 // under
	}

	cfg := memConfig("spike-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Let many samples run (~3s ≫ interval), then assert no restart happened.
	time.Sleep(3 * time.Second)

	var restarts int
	for _, info := range mgr.List() {
		if info.Name == "spike-app" {
			restarts = info.RestartCount
		}
	}
	_ = mgr.Stop("spike-app")

	if restarts != 0 {
		t.Errorf("restart count = %d, want 0 (an alternating spike must not restart)", restarts)
	}
}

// ==================== OOM loop keeps the application serving ====================

// This reverses the original contract, which was that "a genuinely runaway
// process eventually stops instead of flapping forever" — a side effect of
// routing memory restarts through the crash path rather than an independent
// safety requirement.
//
// For a process behind a proxy, stopping is the worst available outcome: DPM
// removes the only listener and nginx returns 502 for every request until a
// human deploys. That is not hypothetical — commerce-v1 storefronts run above
// their ceiling continuously, so they spend the crash budget on recycles alone
// and reach maxRestarts having never once failed.
//
// The anti-thrash property is kept, but it comes from the warm-up window, not
// from the cap: monitorMemory waits memWarmup after every start before it can
// fire again, so recycles are bounded to roughly one per warm-up period no
// matter how fat the process is. In production that is 60s.
func TestMemoryOOMLoopKeepsServing(t *testing.T) {
	mgr, _ := testManager(t)

	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 2
	mgr.memFloor = 1
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 } // always over

	cfg := memConfig("oom-app", "sleep 300", "100MB")
	cfg.MaxRestarts = 3 // the crash cap a recycle must no longer consume
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Recycle well past MaxRestarts. Under the old contract the process would
	// have been stopped at 3; it must still be coming back here.
	deadline := time.Now().Add(20 * time.Second)
	var recycles, restarts int
	var everStopped bool
	for time.Now().Before(deadline) {
		for _, info := range mgr.List() {
			if info.Name != "oom-app" {
				continue
			}
			if info.MemoryRecycles > recycles {
				recycles = info.MemoryRecycles
			}
			if info.RestartCount > restarts {
				restarts = info.RestartCount
			}
			if info.Status == StatusStopped {
				everStopped = true
			}
		}
		if recycles > cfg.MaxRestarts*2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = mgr.Stop("oom-app")

	if recycles <= cfg.MaxRestarts {
		t.Errorf("memory recycles = %d, want > MaxRestarts (%d): the crash cap must not stop a recycle loop",
			recycles, cfg.MaxRestarts)
	}
	if everStopped {
		t.Error("process reached stopped: an over-limit application must keep being replaced, not left dead")
	}
	if restarts != 0 {
		t.Errorf("restart count = %d, want 0 (recycles are not crashes)", restarts)
	}
}

// ==================== A recycle does not pay the crash backoff ====================

// restartBackoff climbs to 30s past ten restarts. A single-instance site is
// serving nothing for that entire window, so a planned recycle must not wait
// it out.
//
// The assertion is a budget rather than a stopwatch. A cycle here costs about
// 2.5s regardless of this change, because monitor sleeps 2s before marking the
// process online and only calls Wait() afterwards — that delay is bookkeeping,
// not downtime, since the process is already accepting connections. Six cycles
// therefore need ~15s now. Under the old backoff the sleeps alone would be
// 1+2+2+5+5+10 = 25s on top of that, so six inside this window is only
// reachable without the backoff.
func TestMemoryRecycleSkipsCrashBackoff(t *testing.T) {
	mgr, _ := testManager(t)

	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 1
	mgr.memFloor = 1
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 }

	cfg := memConfig("fast-recycle-app", "sleep 300", "100MB")
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	const want = 6
	start := time.Now()
	deadline := start.Add(20 * time.Second)
	var recycles int
	for time.Now().Before(deadline) {
		for _, info := range mgr.List() {
			if info.Name == "fast-recycle-app" && info.MemoryRecycles > recycles {
				recycles = info.MemoryRecycles
			}
		}
		if recycles >= want {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = mgr.Stop("fast-recycle-app")

	if recycles < want {
		t.Errorf("memory recycles = %d in 20s, want >= %d (recycles appear to be paying restartBackoff)",
			recycles, want)
	}
	if elapsed := time.Since(start); recycles >= want && elapsed > 18*time.Second {
		t.Errorf("reached %d recycles only after %s — too close to the budget to prove anything", recycles, elapsed)
	}
}
