package deploy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/depfloy/dpm/internal/health"
	"github.com/depfloy/dpm/internal/process"
	"github.com/depfloy/dpm/pkg/config"
)

// Result represents the outcome of a deploy operation.
type Result struct {
	Status             string        `json:"status"` // success, failed, rolled_back
	PreviousRelease    string        `json:"previous_release,omitempty"`
	NewRelease         string        `json:"new_release,omitempty"`
	SwitchDuration     time.Duration `json:"switch_duration_ns"`
	HealthChecksPassed int           `json:"health_checks_passed"`
	HealthChecksFailed int           `json:"health_checks_failed"`
	WorkersRestarted   int           `json:"workers_restarted"`
	Error              string        `json:"error,omitempty"`
}

// Orchestrator manages zero-downtime deployment operations.
type Orchestrator struct {
	pm      *process.Manager
	checker *health.Checker
}

// NewOrchestrator creates a new deploy orchestrator.
func NewOrchestrator(pm *process.Manager, checker *health.Checker) *Orchestrator {
	return &Orchestrator{
		pm:      pm,
		checker: checker,
	}
}

// Switch performs an atomic symlink swap and rolling restart.
// In cluster mode, restarts workers in batches (half at a time) with connection draining.
func (o *Orchestrator) Switch(appName, releasePath, symlinkPath string, ports []int, cfg *config.ProcessConfig) *Result {
	start := time.Now()
	result := &Result{NewRelease: releasePath}

	// 1. Validate new release
	if _, err := os.Stat(releasePath); os.IsNotExist(err) {
		result.Status = "failed"
		result.Error = fmt.Sprintf("release path does not exist: %s", releasePath)
		return result
	}

	// 2. Read current symlink target (for rollback)
	previousRelease, _ := os.Readlink(symlinkPath)
	result.PreviousRelease = previousRelease

	// 3. Atomic symlink swap
	if err := atomicSymlinkSwap(symlinkPath, releasePath); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("symlink swap failed: %v", err)
		return result
	}

	// 4. Rolling restart
	drainTimeout := parseDuration(cfg.DrainTimeout(), 30*time.Second)

	if cfg.IsClusterMode() && len(ports) > 2 {
		// Cluster mode: restart in two batches (half at a time)
		if err := o.clusterRestart(appName, ports, cfg, drainTimeout, result); err != nil {
			o.rollback(appName, symlinkPath, previousRelease, ports, cfg)
			result.Status = "rolled_back"
			result.Error = err.Error()
			result.SwitchDuration = time.Since(start)
			return result
		}
	} else {
		// Legacy mode: restart one by one
		if err := o.sequentialRestart(appName, ports, cfg, drainTimeout, result); err != nil {
			o.rollback(appName, symlinkPath, previousRelease, ports, cfg)
			result.Status = "rolled_back"
			result.Error = err.Error()
			result.SwitchDuration = time.Since(start)
			return result
		}
	}

	result.Status = "success"
	result.SwitchDuration = time.Since(start)
	return result
}

// clusterRestart restarts workers in two batches with connection draining.
// Batch 1 drains and restarts first half while second half serves traffic.
// Batch 2 drains and restarts second half while first half (new code) serves traffic.
func (o *Orchestrator) clusterRestart(appName string, ports []int, cfg *config.ProcessConfig, drainTimeout time.Duration, result *Result) error {
	mid := len(ports) / 2
	batch1 := ports[:mid]  // First half
	batch2 := ports[mid:]  // Second half

	// Batch 1: drain + stop + start first half
	if err := o.restartBatch(appName, batch1, cfg, drainTimeout, result); err != nil {
		return fmt.Errorf("batch 1 failed: %w", err)
	}

	// Batch 2: drain + stop + start second half
	if err := o.restartBatch(appName, batch2, cfg, drainTimeout, result); err != nil {
		return fmt.Errorf("batch 2 failed: %w", err)
	}

	return nil
}

// restartBatch drains, stops, and restarts a batch of workers.
func (o *Orchestrator) restartBatch(appName string, ports []int, cfg *config.ProcessConfig, drainTimeout time.Duration, result *Result) error {
	// 1. Drain: wait for active requests to complete
	//    In a real nginx integration, we'd mark these as "down" in upstream first.
	//    For now, we wait drainTimeout before stopping.
	time.Sleep(drainTimeout)

	// 2. Stop all workers in this batch
	for i, port := range ports {
		instanceName := fmt.Sprintf("%s:%d", appName, i+portOffset(ports, cfg))
		_ = o.pm.Stop(instanceName)
		_ = port // port used for health check below
	}

	// 3. Wait for ports to free
	time.Sleep(1 * time.Second)

	// 4. Start all workers with new code
	for i, port := range ports {
		instanceCfg := *cfg
		instanceCfg.Instances = 1
		instanceCfg.Cluster = nil // Don't recurse cluster in sub-instances

		if err := o.pm.Start(&instanceCfg, []int{port}); err != nil {
			return fmt.Errorf("failed to start worker on port %d: %w", port, err)
		}

		// 5. Health check
		if cfg.HealthCheck != nil {
			if o.waitForHealthy(port, cfg.HealthCheck, 15, 2*time.Second) {
				result.HealthChecksPassed++
			} else {
				result.HealthChecksFailed++
				return fmt.Errorf("health check failed for worker on port %d", port)
			}
		} else {
			time.Sleep(3 * time.Second)
			result.HealthChecksPassed++
		}

		result.WorkersRestarted++
		_ = i
	}

	return nil
}

// sequentialRestart restarts workers one by one (legacy 2-instance mode).
func (o *Orchestrator) sequentialRestart(appName string, ports []int, cfg *config.ProcessConfig, drainTimeout time.Duration, result *Result) error {
	for i, port := range ports {
		instanceName := appName
		if len(ports) > 1 {
			instanceName = fmt.Sprintf("%s:%d", appName, i)
		}

		// Drain wait
		if drainTimeout > 0 && cfg.IsClusterMode() {
			time.Sleep(drainTimeout)
		}

		_ = o.pm.Stop(instanceName)
		time.Sleep(1 * time.Second)

		instanceCfg := *cfg
		instanceCfg.Instances = 1
		instanceCfg.Cluster = nil

		if err := o.pm.Start(&instanceCfg, []int{port}); err != nil {
			return fmt.Errorf("failed to start instance %d: %w", i, err)
		}

		if cfg.HealthCheck != nil {
			if o.waitForHealthy(port, cfg.HealthCheck, 15, 2*time.Second) {
				result.HealthChecksPassed++
			} else {
				result.HealthChecksFailed++
				return fmt.Errorf("health check failed for instance %d on port %d", i, port)
			}
		} else {
			time.Sleep(3 * time.Second)
			result.HealthChecksPassed++
		}

		result.WorkersRestarted++
	}

	return nil
}

// Rollback reverts to the previous release.
func (o *Orchestrator) Rollback(appName, symlinkPath string, ports []int, cfg *config.ProcessConfig) *Result {
	start := time.Now()
	result := &Result{}

	currentRelease, _ := os.Readlink(symlinkPath)
	result.NewRelease = currentRelease

	releasesDir := filepath.Dir(symlinkPath) + "/releases"
	entries, err := os.ReadDir(releasesDir)
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("cannot read releases dir: %v", err)
		return result
	}

	var previousRelease string
	for i := len(entries) - 1; i >= 0; i-- {
		fullPath := filepath.Join(releasesDir, entries[i].Name())
		if fullPath != currentRelease {
			previousRelease = fullPath
			break
		}
	}

	if previousRelease == "" {
		result.Status = "failed"
		result.Error = "no previous release found"
		return result
	}

	result.PreviousRelease = previousRelease
	o.rollback(appName, symlinkPath, previousRelease, ports, cfg)

	result.Status = "success"
	result.SwitchDuration = time.Since(start)
	return result
}

func (o *Orchestrator) rollback(appName, symlinkPath, previousRelease string, ports []int, cfg *config.ProcessConfig) {
	if previousRelease == "" {
		return
	}
	atomicSymlinkSwap(symlinkPath, previousRelease)
	o.pm.Stop(appName)
	time.Sleep(1 * time.Second)
	o.pm.Start(cfg, ports)
}

func (o *Orchestrator) waitForHealthy(port int, cfg *config.HealthCheckConfig, maxRetries int, delay time.Duration) bool {
	for i := 0; i < maxRetries; i++ {
		status := o.checker.CheckOnce(port, cfg)
		if status.Healthy {
			return true
		}
		time.Sleep(delay)
	}
	return false
}

func atomicSymlinkSwap(symlinkPath, targetPath string) error {
	tmpLink := symlinkPath + ".new"
	os.Remove(tmpLink)
	if err := os.Symlink(targetPath, tmpLink); err != nil {
		return fmt.Errorf("create temp symlink: %w", err)
	}
	if err := os.Rename(tmpLink, symlinkPath); err != nil {
		os.Remove(tmpLink)
		return fmt.Errorf("atomic rename: %w", err)
	}
	return nil
}

func parseDuration(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}

// portOffset calculates the global instance offset for a batch of ports.
func portOffset(batchPorts []int, cfg *config.ProcessConfig) int {
	// Simple: use port number to derive offset
	return 0
}

// JSON returns the result as a JSON string.
func (r *Result) JSON() string {
	data, _ := json.Marshal(r)
	return string(data)
}
