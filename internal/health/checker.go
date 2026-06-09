package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/depfloy/dpm/pkg/config"
)

// Status represents the result of a health check.
type Status struct {
	Healthy      bool          `json:"healthy"`
	CheckType    string        `json:"check_type"`
	Message      string        `json:"message,omitempty"`
	ResponseTime time.Duration `json:"response_time_ns"`
	LastCheck    time.Time     `json:"last_check"`
	Consecutive  int           `json:"consecutive"` // Consecutive pass/fail count
}

// Checker performs health checks on processes.
type Checker struct {
	mu       sync.RWMutex
	statuses map[string]*Status // key: process name
	stopChs  map[string]chan struct{}
	doneChs  map[string]chan struct{}
	client   *http.Client
	onUnhealthy func(name string, status *Status)
	onHealthy   func(name string, status *Status)
}

// NewChecker creates a new health checker.
func NewChecker() *Checker {
	return &Checker{
		statuses: make(map[string]*Status),
		stopChs:  make(map[string]chan struct{}),
		doneChs:  make(map[string]chan struct{}),
		client: &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// OnUnhealthy registers a callback for when a process becomes unhealthy.
func (c *Checker) OnUnhealthy(fn func(name string, status *Status)) {
	c.onUnhealthy = fn
}

// OnHealthy registers a callback for when a process recovers to healthy.
func (c *Checker) OnHealthy(fn func(name string, status *Status)) {
	c.onHealthy = fn
}

// StartMonitoring begins periodic health checks for a process.
func (c *Checker) StartMonitoring(name string, port int, cfg *config.HealthCheckConfig) {
	if cfg == nil {
		return
	}

	// Stop any existing monitor and capture its done channel under the lock, so we
	// can wait for the old goroutine to fully exit before starting a new one. This
	// prevents two goroutines concurrently writing c.statuses[name].
	c.mu.Lock()
	if stopCh, ok := c.stopChs[name]; ok {
		close(stopCh)
		delete(c.stopChs, name)
	}
	oldDone := c.doneChs[name]
	delete(c.doneChs, name)
	c.mu.Unlock()

	if oldDone != nil {
		select {
		case <-oldDone:
		case <-time.After(10 * time.Second):
		}
	}

	interval, _ := time.ParseDuration(cfg.Interval)
	if interval == 0 {
		interval = 10 * time.Second
	}

	stopCh := make(chan struct{})
	newDoneCh := make(chan struct{})
	c.mu.Lock()
	c.stopChs[name] = stopCh
	c.doneChs[name] = newDoneCh
	c.statuses[name] = &Status{Healthy: true, CheckType: cfg.Type}
	c.mu.Unlock()

	go func() {
		defer close(newDoneCh)
		// Initial delay to let process start
		time.Sleep(5 * time.Second)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		healthyThreshold := cfg.HealthyThreshold
		if healthyThreshold == 0 {
			healthyThreshold = 2
		}
		unhealthyThreshold := cfg.UnhealthyThreshold
		if unhealthyThreshold == 0 {
			unhealthyThreshold = 3
		}

		consecutivePass := 0
		consecutiveFail := 0

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				result := c.check(cfg, port)
				result.LastCheck = time.Now()

				c.mu.Lock()
				prev := c.statuses[name]
				wasHealthy := prev != nil && prev.Healthy

				if result.Healthy {
					consecutivePass++
					consecutiveFail = 0
					result.Consecutive = consecutivePass
					if consecutivePass >= healthyThreshold {
						result.Healthy = true
						if !wasHealthy && c.onHealthy != nil {
							go c.onHealthy(name, result)
						}
					} else {
						result.Healthy = wasHealthy
					}
				} else {
					consecutiveFail++
					consecutivePass = 0
					result.Consecutive = consecutiveFail
					if consecutiveFail >= unhealthyThreshold {
						result.Healthy = false
						if wasHealthy && c.onUnhealthy != nil {
							go c.onUnhealthy(name, result)
						}
					} else {
						result.Healthy = wasHealthy
					}
				}

				c.statuses[name] = result
				c.mu.Unlock()
			}
		}
	}()
}

// StopMonitoring stops health checks for a process.
func (c *Checker) StopMonitoring(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stopCh, ok := c.stopChs[name]; ok {
		close(stopCh)
		delete(c.stopChs, name)
	}
	// Clean up the remaining per-process entries so the maps don't grow unbounded
	// as processes are created and removed over the daemon's lifetime.
	delete(c.doneChs, name)
	delete(c.statuses, name)
}

// GetStatus returns the current health status of a process.
func (c *Checker) GetStatus(name string) *Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.statuses[name]
}

// GetAllStatuses returns health statuses for all monitored processes.
func (c *Checker) GetAllStatuses() map[string]*Status {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*Status, len(c.statuses))
	for k, v := range c.statuses {
		result[k] = v
	}
	return result
}

// CheckOnce performs a single health check and returns the result.
func (c *Checker) CheckOnce(port int, cfg *config.HealthCheckConfig) *Status {
	if cfg == nil {
		return &Status{Healthy: true, Message: "no health check configured"}
	}
	result := c.check(cfg, port)
	result.LastCheck = time.Now()
	return result
}

// check performs a single health check based on config type.
func (c *Checker) check(cfg *config.HealthCheckConfig, port int) *Status {
	timeout, _ := time.ParseDuration(cfg.Timeout)
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	start := time.Now()

	switch cfg.Type {
	case "http":
		return c.checkHTTP(cfg.Path, port, timeout, start)
	case "tcp":
		return c.checkTCP(port, timeout, start)
	case "exec":
		return c.checkExec(cfg.Command, timeout, start)
	default:
		return c.checkTCP(port, timeout, start)
	}
}

func (c *Checker) checkHTTP(path string, port int, timeout time.Duration, start time.Time) *Status {
	if path == "" {
		path = "/"
	}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)

	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(url)
	elapsed := time.Since(start)

	if err != nil {
		return &Status{
			Healthy:      false,
			CheckType:    "http",
			Message:      fmt.Sprintf("request failed: %v", err),
			ResponseTime: elapsed,
		}
	}
	defer resp.Body.Close()

	// 2xx, 3xx, 4xx are considered healthy (app is responding)
	// 5xx means unhealthy
	healthy := resp.StatusCode < 500
	msg := fmt.Sprintf("HTTP %d", resp.StatusCode)

	return &Status{
		Healthy:      healthy,
		CheckType:    "http",
		Message:      msg,
		ResponseTime: elapsed,
	}
}

func (c *Checker) checkTCP(port int, timeout time.Duration, start time.Time) *Status {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	elapsed := time.Since(start)

	if err != nil {
		return &Status{
			Healthy:      false,
			CheckType:    "tcp",
			Message:      fmt.Sprintf("connection failed: %v", err),
			ResponseTime: elapsed,
		}
	}
	conn.Close()

	return &Status{
		Healthy:      true,
		CheckType:    "tcp",
		Message:      "connected",
		ResponseTime: elapsed,
	}
}

func (c *Checker) checkExec(command string, timeout time.Duration, start time.Time) *Status {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	// Run in its own process group so the timeout kills the whole tree, not just
	// the shell - otherwise child processes spawned by the command are orphaned.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// On timeout, os/exec invokes Cancel (only after Start, so cmd.Process is set
	// and safe to read here - no concurrent access). Kill the whole group.
	cmd.Cancel = func() error {
		if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil {
			return syscall.Kill(-pgid, syscall.SIGKILL)
		}
		return cmd.Process.Kill()
	}

	// Run blocks in this goroutine until the command exits or ctx kills it, so
	// there is no concurrent access to cmd's fields.
	err := cmd.Run()
	elapsed := time.Since(start)

	if ctx.Err() == context.DeadlineExceeded {
		return &Status{
			Healthy:      false,
			CheckType:    "exec",
			Message:      "timeout",
			ResponseTime: timeout,
		}
	}
	if err != nil {
		return &Status{
			Healthy:      false,
			CheckType:    "exec",
			Message:      fmt.Sprintf("command failed: %v", err),
			ResponseTime: elapsed,
		}
	}
	return &Status{
		Healthy:      true,
		CheckType:    "exec",
		Message:      "exit 0",
		ResponseTime: elapsed,
	}
}
