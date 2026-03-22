package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DaemonConfig is the global DPM daemon configuration.
type DaemonConfig struct {
	Daemon struct {
		Socket  string `yaml:"socket"`
		PIDFile string `yaml:"pid_file"`
		LogFile string `yaml:"log_file"`
	} `yaml:"daemon"`

	User string `yaml:"user"`

	Ports PortRanges `yaml:"ports"`

	Logging LoggingConfig `yaml:"logging"`

	Nginx NginxConfig `yaml:"nginx"`

	HealthCheck HealthCheckDefaults `yaml:"health_check"`

	State struct {
		Dir string `yaml:"dir"`
	} `yaml:"state"`
}

// PortRanges defines port allocation ranges by process type.
type PortRanges struct {
	NodeJS  [2]int `yaml:"nodejs"`
	Plugins [2]int `yaml:"plugins"`
	Workers [2]int `yaml:"workers"`
}

// LoggingConfig defines log rotation and format settings.
type LoggingConfig struct {
	Format   string         `yaml:"format"`
	Dir      string         `yaml:"dir"`
	Rotation RotationConfig `yaml:"rotation"`
}

// RotationConfig defines log rotation parameters.
type RotationConfig struct {
	MaxSize    string `yaml:"max_size"`
	MaxAge     string `yaml:"max_age"`
	MaxBackups int    `yaml:"max_backups"`
	Compress   bool   `yaml:"compress"`
}

// NginxConfig defines nginx management settings.
type NginxConfig struct {
	Mode          string `yaml:"mode"` // "external" or "managed"
	ConfigDir     string `yaml:"config_dir"`
	ReloadCommand string `yaml:"reload_command"`
}

// HealthCheckDefaults defines default health check parameters.
type HealthCheckDefaults struct {
	Interval string `yaml:"default_interval"`
	Timeout  string `yaml:"default_timeout"`
	Retries  int    `yaml:"default_retries"`
}

// ProcessConfig defines a single managed process.
type ProcessConfig struct {
	Type    string `yaml:"type" json:"type"`       // nodejs, php, static, worker
	Name    string `yaml:"name" json:"name"`
	Command string `yaml:"command" json:"command"`
	CWD     string `yaml:"cwd" json:"cwd"`
	User    string `yaml:"user,omitempty" json:"user,omitempty"`

	Instances int    `yaml:"instances,omitempty" json:"instances,omitempty"`
	Port      string `yaml:"port,omitempty" json:"port,omitempty"` // "auto" or specific port number

	Env map[string]string `yaml:"env,omitempty" json:"env,omitempty"`

	HealthCheck *HealthCheckConfig `yaml:"health_check,omitempty" json:"health_check,omitempty"`

	Resources *ResourceLimits `yaml:"resources,omitempty" json:"resources,omitempty"`

	RestartPolicy string `yaml:"restart_policy,omitempty" json:"restart_policy,omitempty"` // always, on-failure, never
	RestartDelay  string `yaml:"restart_delay,omitempty" json:"restart_delay,omitempty"`
	MaxRestarts   int    `yaml:"max_restarts,omitempty" json:"max_restarts,omitempty"`
	StopSignal    string `yaml:"stop_signal,omitempty" json:"stop_signal,omitempty"`     // SIGTERM, SIGKILL, SIGINT, SIGQUIT
	StopTimeout   string `yaml:"stop_timeout,omitempty" json:"stop_timeout,omitempty"`   // e.g. "10s"

	Nginx *ProcessNginxConfig `yaml:"nginx,omitempty" json:"nginx,omitempty"`

	Workers []WorkerConfig `yaml:"workers,omitempty" json:"workers,omitempty"`
}

// HealthCheckConfig defines health check settings for a process.
type HealthCheckConfig struct {
	Type               string `yaml:"type" json:"type"` // http, tcp, exec
	Path               string `yaml:"path,omitempty" json:"path,omitempty"`
	Command            string `yaml:"command,omitempty" json:"command,omitempty"`
	Interval           string `yaml:"interval,omitempty" json:"interval,omitempty"`
	Timeout            string `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	HealthyThreshold   int    `yaml:"healthy_threshold,omitempty" json:"healthy_threshold,omitempty"`
	UnhealthyThreshold int    `yaml:"unhealthy_threshold,omitempty" json:"unhealthy_threshold,omitempty"`
}

// ResourceLimits defines resource constraints for a process.
type ResourceLimits struct {
	MaxMemory string `yaml:"max_memory,omitempty" json:"max_memory,omitempty"`
	MaxCPU    int    `yaml:"max_cpu,omitempty" json:"max_cpu,omitempty"`
}

// ProcessNginxConfig defines nginx settings for a process.
type ProcessNginxConfig struct {
	Domains     []string `yaml:"domains,omitempty" json:"domains,omitempty"`
	SSL         string   `yaml:"ssl,omitempty" json:"ssl,omitempty"`
	WWWRedirect string   `yaml:"www_redirect,omitempty" json:"www_redirect,omitempty"`
	WebSocket   bool     `yaml:"websocket,omitempty" json:"websocket,omitempty"`
	SPA         bool     `yaml:"spa,omitempty" json:"spa,omitempty"`
	ReplaceFPM  bool     `yaml:"replace_fpm,omitempty" json:"replace_fpm,omitempty"`
	Path        string   `yaml:"path,omitempty" json:"path,omitempty"`
}

// WorkerConfig defines a sub-worker process attached to a main process.
type WorkerConfig struct {
	Name            string              `yaml:"name" json:"name"`
	Command         string              `yaml:"command" json:"command"`
	Port            string              `yaml:"port,omitempty" json:"port,omitempty"`
	RestartOnDeploy bool                `yaml:"restart_on_deploy,omitempty" json:"restart_on_deploy,omitempty"`
	Nginx           *ProcessNginxConfig `yaml:"nginx,omitempty" json:"nginx,omitempty"`
}

// DefaultDaemonConfig returns a DaemonConfig with sensible defaults.
func DefaultDaemonConfig() *DaemonConfig {
	cfg := &DaemonConfig{}
	cfg.Daemon.Socket = "/var/run/dpm/dpm.sock"
	cfg.Daemon.PIDFile = "/var/run/dpm/dpm.pid"
	cfg.Daemon.LogFile = "/var/log/dpm/daemon.log"
	cfg.User = "depfloy"
	cfg.Ports.NodeJS = [2]int{3000, 4999}
	cfg.Ports.Plugins = [2]int{5000, 5999}
	cfg.Ports.Workers = [2]int{6000, 6999}
	cfg.Logging.Format = "json"
	cfg.Logging.Dir = "/var/log/dpm"
	cfg.Logging.Rotation.MaxSize = "100MB"
	cfg.Logging.Rotation.MaxAge = "30d"
	cfg.Logging.Rotation.MaxBackups = 10
	cfg.Logging.Rotation.Compress = true
	cfg.Nginx.Mode = "external"
	cfg.Nginx.ConfigDir = "/etc/nginx"
	cfg.Nginx.ReloadCommand = "nginx -t && nginx -s reload"
	cfg.HealthCheck.Interval = "10s"
	cfg.HealthCheck.Timeout = "5s"
	cfg.HealthCheck.Retries = 3
	cfg.State.Dir = "/var/lib/dpm"
	return cfg
}

// LoadDaemonConfig reads and parses a YAML config file, applying defaults for missing values.
func LoadDaemonConfig(path string) (*DaemonConfig, error) {
	cfg := DefaultDaemonConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

// LoadProcessConfig reads and parses a process YAML config file.
func LoadProcessConfig(path string) (*ProcessConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read process config: %w", err)
	}

	cfg := &ProcessConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse process config: %w", err)
	}

	if cfg.RestartPolicy == "" {
		cfg.RestartPolicy = "always"
	}
	if cfg.Instances == 0 {
		cfg.Instances = 1
	}

	return cfg, nil
}

// ParseProcessConfigJSON parses process config from JSON bytes.
func ParseProcessConfigJSON(data []byte) (*ProcessConfig, error) {
	cfg := &ProcessConfig{}

	// yaml.v3 can parse JSON
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse process config json: %w", err)
	}

	if cfg.RestartPolicy == "" {
		cfg.RestartPolicy = "always"
	}
	if cfg.Instances == 0 {
		cfg.Instances = 1
	}

	return cfg, nil
}
