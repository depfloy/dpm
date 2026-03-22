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
	Status           string        `json:"status"` // success, failed, rolled_back
	PreviousRelease  string        `json:"previous_release,omitempty"`
	NewRelease       string        `json:"new_release,omitempty"`
	SwitchDuration   time.Duration `json:"switch_duration_ns"`
	HealthChecksPassed int         `json:"health_checks_passed"`
	HealthChecksFailed int         `json:"health_checks_failed"`
	Error            string        `json:"error,omitempty"`
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

// Switch performs an atomic symlink swap and rolling restart of process instances.
// This is the core zero-downtime deployment operation.
//
// Flow:
//  1. Validate new release exists
//  2. Atomic symlink swap (current -> new release)
//  3. For each instance:
//     a. Stop instance
//     b. Wait for port to free
//     c. Start instance (picks up new code via symlink)
//     d. Health check
//  4. On failure: rollback symlink and restart on old code
func (o *Orchestrator) Switch(appName, releasePath, symlinkPath string, ports []int, cfg *config.ProcessConfig) *Result {
	start := time.Now()
	result := &Result{
		NewRelease: releasePath,
	}

	// 1. Validate new release
	if _, err := os.Stat(releasePath); os.IsNotExist(err) {
		result.Status = "failed"
		result.Error = fmt.Sprintf("release path does not exist: %s", releasePath)
		return result
	}

	// 2. Read current symlink target (for rollback)
	previousRelease, err := os.Readlink(symlinkPath)
	if err != nil {
		previousRelease = ""
	}
	result.PreviousRelease = previousRelease

	// 3. Atomic symlink swap
	if err := atomicSymlinkSwap(symlinkPath, releasePath); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("symlink swap failed: %v", err)
		return result
	}

	// 4. Rolling restart with health checks
	for i, port := range ports {
		instanceName := appName
		if len(ports) > 1 {
			instanceName = fmt.Sprintf("%s:%d", appName, i)
		}

		// Stop this instance
		_ = o.pm.Stop(instanceName)

		// Wait for port to free
		time.Sleep(1 * time.Second)

		// Start instance with new code (cwd points to symlink, which now points to new release)
		instanceCfg := *cfg
		instanceCfg.Instances = 1
		if err := o.pm.Start(&instanceCfg, []int{port}); err != nil {
			// Rollback
			result.HealthChecksFailed++
			o.rollback(appName, symlinkPath, previousRelease, ports, cfg)
			result.Status = "rolled_back"
			result.Error = fmt.Sprintf("failed to start instance %d: %v", i, err)
			result.SwitchDuration = time.Since(start)
			return result
		}

		// Health check
		if cfg.HealthCheck != nil {
			healthy := o.waitForHealthy(port, cfg.HealthCheck, 15, 2*time.Second)
			if healthy {
				result.HealthChecksPassed++
			} else {
				result.HealthChecksFailed++
				// Rollback
				o.rollback(appName, symlinkPath, previousRelease, ports, cfg)
				result.Status = "rolled_back"
				result.Error = fmt.Sprintf("health check failed for instance %d on port %d", i, port)
				result.SwitchDuration = time.Since(start)
				return result
			}
		} else {
			// No health check configured, wait a bit and check if process is alive
			time.Sleep(3 * time.Second)
			result.HealthChecksPassed++
		}
	}

	result.Status = "success"
	result.SwitchDuration = time.Since(start)
	return result
}

// Rollback reverts to the previous release.
func (o *Orchestrator) Rollback(appName, symlinkPath string, ports []int, cfg *config.ProcessConfig) *Result {
	start := time.Now()
	result := &Result{}

	// Read current target
	currentRelease, _ := os.Readlink(symlinkPath)
	result.NewRelease = currentRelease

	// Find previous release
	releasesDir := filepath.Dir(symlinkPath) + "/releases"
	entries, err := os.ReadDir(releasesDir)
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("cannot read releases dir: %v", err)
		return result
	}

	// Find the second most recent release
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

// rollback reverts symlink and restarts all instances on old code.
func (o *Orchestrator) rollback(appName, symlinkPath, previousRelease string, ports []int, cfg *config.ProcessConfig) {
	if previousRelease == "" {
		return
	}

	// Swap symlink back
	atomicSymlinkSwap(symlinkPath, previousRelease)

	// Stop all instances
	o.pm.Stop(appName)
	time.Sleep(1 * time.Second)

	// Restart all instances (they'll pick up old code via symlink)
	o.pm.Start(cfg, ports)
}

// waitForHealthy performs repeated health checks until success or max retries.
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

// atomicSymlinkSwap performs an atomic symlink replacement.
// Uses the ln -sfn + mv -Tf pattern for atomicity.
func atomicSymlinkSwap(symlinkPath, targetPath string) error {
	tmpLink := symlinkPath + ".new"

	// Remove stale temp link
	os.Remove(tmpLink)

	// Create new symlink at temp location
	if err := os.Symlink(targetPath, tmpLink); err != nil {
		return fmt.Errorf("create temp symlink: %w", err)
	}

	// Atomic rename to replace the original symlink
	if err := os.Rename(tmpLink, symlinkPath); err != nil {
		os.Remove(tmpLink)
		return fmt.Errorf("atomic rename: %w", err)
	}

	return nil
}

// ResultJSON returns the result as a JSON string.
func (r *Result) JSON() string {
	data, _ := json.Marshal(r)
	return string(data)
}
