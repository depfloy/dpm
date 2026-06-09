package health

import (
	"testing"
	"time"
)

// TestStopMonitoringCleansMaps verifies that StopMonitoring removes the
// per-process entries from all internal maps, so they don't grow unbounded as
// processes are created and removed over the daemon's lifetime.
func TestStopMonitoringCleansMaps(t *testing.T) {
	c := NewChecker()

	// Simulate an active monitor's bookkeeping without spinning up the goroutine.
	c.mu.Lock()
	c.stopChs["app"] = make(chan struct{})
	c.doneChs["app"] = make(chan struct{})
	c.statuses["app"] = &Status{Healthy: true}
	c.mu.Unlock()

	c.StopMonitoring("app")

	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.stopChs["app"]; ok {
		t.Error("stopChs entry not removed")
	}
	if _, ok := c.doneChs["app"]; ok {
		t.Error("doneChs entry not removed")
	}
	if _, ok := c.statuses["app"]; ok {
		t.Error("statuses entry not removed")
	}
}

// TestCheckExecTimeoutReturnsQuickly verifies that an exec health check whose
// command hangs is aborted at the timeout (rather than blocking indefinitely)
// and reports unhealthy.
func TestCheckExecTimeoutReturnsQuickly(t *testing.T) {
	c := NewChecker()

	start := time.Now()
	status := c.checkExec("sleep 30", 300*time.Millisecond, start)
	elapsed := time.Since(start)

	if status.Healthy {
		t.Error("hanging command should be reported unhealthy")
	}
	if status.Message != "timeout" {
		t.Errorf("message = %q, want timeout", status.Message)
	}
	// Should return shortly after the timeout, not after the full 30s sleep.
	if elapsed > 5*time.Second {
		t.Errorf("checkExec took %v, expected to abort near the 300ms timeout", elapsed)
	}
}

// TestCheckExecSuccessAndFailure verifies the basic pass/fail paths.
func TestCheckExecSuccessAndFailure(t *testing.T) {
	c := NewChecker()

	if s := c.checkExec("exit 0", time.Second, time.Now()); !s.Healthy {
		t.Errorf("exit 0 should be healthy, got %q", s.Message)
	}
	if s := c.checkExec("exit 1", time.Second, time.Now()); s.Healthy {
		t.Error("exit 1 should be unhealthy")
	}
}
