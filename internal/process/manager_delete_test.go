package process

import (
	"testing"
	"time"

	"github.com/depfloy/dpm/pkg/config"
)

// TestDeleteReapsParkedDrainWorkers guards DP-98 (Server 46 incident).
//
// A blue-green deploy parks the old worker in pendingDrain until Drain() is
// called. If Delete() ignores pendingDrain, the parked worker's monitor
// (RestartPolicy "always") resurrects it after the delete — exactly the
// incident where app_235 came back on its own after `dpm delete`. Delete must
// signal-stop and reap parked workers too.
func TestDeleteReapsParkedDrainWorkers(t *testing.T) {
	mgr, _ := testManager(t)

	cfg := testConfig("reap-app", "sleep 300")
	cfg.Cluster = &config.ClusterConfig{Mode: "fixed", Workers: 1, DrainTimeout: "1s"}
	cfg.Instances = 1

	if err := mgr.Start(cfg, []int{9300}); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Blue-green deploy → old worker parks in pendingDrain (Drain NOT called).
	done := make(chan struct{})
	var deployErr error
	go func() {
		_, deployErr = mgr.Deploy(cfg, []int{9400})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("deploy timed out - possible deadlock")
	}
	if deployErr != nil {
		t.Fatalf("deploy: %v", deployErr)
	}

	// Capture the parked worker before deleting.
	mgr.mu.RLock()
	parked := append([]*managed(nil), mgr.pendingDrain["reap-app"]...)
	mgr.mu.RUnlock()
	if len(parked) != 1 {
		t.Fatalf("pendingDrain count = %d, want 1 (deploy should park the old worker)", len(parked))
	}

	// Delete WITHOUT draining first.
	if err := mgr.Delete("reap-app"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// pendingDrain must be cleared.
	mgr.mu.RLock()
	remaining := len(mgr.pendingDrain["reap-app"])
	mgr.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("pendingDrain after delete = %d, want 0", remaining)
	}

	// The parked worker must have been signal-stopped (stopCh closed) so its
	// monitor sees the stop intent and will NOT resurrect it.
	select {
	case <-parked[0].stopCh:
		// closed — good
	default:
		t.Error("parked worker stopCh not closed after Delete; monitor could resurrect it (DP-98)")
	}

	// No managed entries should remain for the name.
	mgr.mu.RLock()
	for key, proc := range mgr.processes {
		if proc.config.Name == "reap-app" {
			t.Errorf("process still present after delete: key=%s", key)
		}
	}
	mgr.mu.RUnlock()
}

// TestStartIsIdempotentForSameName guards DP-98: starting the same name twice
// must not leave two records. Start() stops & removes existing instances first,
// so a re-start replaces rather than duplicates.
func TestStartIsIdempotentForSameName(t *testing.T) {
	mgr, _ := testManager(t)

	cfg := testConfig("idem-app", "sleep 300")
	cfg.Cluster = &config.ClusterConfig{Mode: "fixed", Workers: 1, DrainTimeout: "1s"}
	cfg.Instances = 1

	if err := mgr.Start(cfg, []int{9500}); err != nil {
		t.Fatalf("start 1: %v", err)
	}
	time.Sleep(1 * time.Second)
	if err := mgr.Start(cfg, []int{9600}); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Exactly one record for the name — no duplicate (DP-98). Port behaviour is
	// intentionally not asserted here: port allocation is a separate, sensitive
	// area we don't touch in this change.
	count := 0
	for _, info := range mgr.List() {
		if info.Name == "idem-app" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("instance count = %d, want 1 (Start must be idempotent, not duplicate)", count)
	}

	mgr.Stop("idem-app")
}
