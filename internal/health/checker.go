package health

import (
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"sync"
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
	client   *http.Client
	onUnhealthy func(name string, status *Status)
}

// NewChecker creates a new health checker.
func NewChecker() *Checker {
	return &Checker{
		statuses: make(map[string]*Status),
		stopChs:  make(map[string]chan struct{}),
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

// StartMonitoring begins periodic health checks for a process.
func (c *Checker) StartMonitoring(name string, port int, cfg *config.HealthCheckConfig) {
	if cfg == nil {
		return
	}

	c.StopMonitoring(name)

	interval, _ := time.ParseDuration(cfg.Interval)
	if interval == 0 {
		interval = 10 * time.Second
	}

	stopCh := make(chan struct{})
	c.mu.Lock()
	c.stopChs[name] = stopCh
	c.statuses[name] = &Status{Healthy: true, CheckType: cfg.Type}
	c.mu.Unlock()

	go func() {
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
					} else {
						result.Healthy = wasHealthy // Keep previous state
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
	cmd := exec.Command("sh", "-c", command)
	done := make(chan error, 1)

	go func() {
		done <- cmd.Run()
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
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
	case <-time.After(timeout):
		cmd.Process.Kill()
		return &Status{
			Healthy:      false,
			CheckType:    "exec",
			Message:      "timeout",
			ResponseTime: timeout,
		}
	}
}
