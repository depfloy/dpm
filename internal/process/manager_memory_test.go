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

	// A live process kept over its limit must be restarted repeatedly. Poll for
	// two restarts — comfortably before the fast-crash guard (5) stops it.
	deadline := time.Now().Add(15 * time.Second)
	var restarts int
	for time.Now().Before(deadline) {
		for _, info := range mgr.List() {
			if info.Name == "mem-app" && info.RestartCount > restarts {
				restarts = info.RestartCount
			}
		}
		if restarts >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = mgr.Stop("mem-app")

	if restarts < 2 {
		t.Errorf("restart count = %d, want >= 2 (memory limit should drive restarts)", restarts)
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

// ==================== OOM loop eventually stops (flapping guard) ====================

func TestMemoryOOMLoopEventuallyStops(t *testing.T) {
	mgr, _ := testManager(t)

	mgr.memWarmup = 0
	mgr.memInterval = 30 * time.Millisecond
	mgr.memBreaches = 2
	mgr.memFloor = 1
	mgr.readMemory = func(int) uint64 { return 200 * 1024 * 1024 } // always over

	cfg := memConfig("oom-app", "sleep 300", "100MB")
	cfg.MaxRestarts = 3 // stop after a few memory restarts, deterministically
	if err := mgr.Start(cfg, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A process restarted for memory participates in the restart counters, so an
	// endlessly-fat app hits MaxRestarts and stops instead of flapping forever.
	got := waitForStatus(mgr, "oom-app", StatusStopped, 25*time.Second)
	_ = mgr.Stop("oom-app")

	if got != StatusStopped {
		t.Errorf("status = %q, want stopped (OOM loop must hit the restart cap)", got)
	}
}
