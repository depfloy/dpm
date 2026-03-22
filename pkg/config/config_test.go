package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultDaemonConfig(t *testing.T) {
	cfg := DefaultDaemonConfig()

	if cfg.Daemon.Socket != "/var/run/dpm/dpm.sock" {
		t.Errorf("Socket = %s, want /var/run/dpm/dpm.sock", cfg.Daemon.Socket)
	}
	if cfg.User != "depfloy" {
		t.Errorf("User = %s, want depfloy", cfg.User)
	}
	if cfg.Ports.NodeJS[0] != 3000 || cfg.Ports.NodeJS[1] != 4999 {
		t.Errorf("NodeJS ports = %v, want [3000, 4999]", cfg.Ports.NodeJS)
	}
	if cfg.HealthCheck.Retries != 3 {
		t.Errorf("Retries = %d, want 3", cfg.HealthCheck.Retries)
	}
	if cfg.Nginx.Mode != "external" {
		t.Errorf("Nginx mode = %s, want external", cfg.Nginx.Mode)
	}
}

func TestLoadDaemonConfig(t *testing.T) {
	content := `
daemon:
  socket: /tmp/test.sock
  log_file: /tmp/test.log

user: testuser

ports:
  nodejs: [4000, 4999]
  plugins: [5000, 5099]
  workers: [6000, 6099]

logging:
  format: text
  rotation:
    max_size: 50MB
    compress: false

nginx:
  mode: managed

health_check:
  default_retries: 5
`

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(content), 0644)

	cfg, err := LoadDaemonConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Daemon.Socket != "/tmp/test.sock" {
		t.Errorf("Socket = %s, want /tmp/test.sock", cfg.Daemon.Socket)
	}
	if cfg.User != "testuser" {
		t.Errorf("User = %s, want testuser", cfg.User)
	}
	if cfg.Ports.NodeJS[0] != 4000 {
		t.Errorf("NodeJS start = %d, want 4000", cfg.Ports.NodeJS[0])
	}
	if cfg.Nginx.Mode != "managed" {
		t.Errorf("Nginx mode = %s, want managed", cfg.Nginx.Mode)
	}
	if cfg.HealthCheck.Retries != 5 {
		t.Errorf("Retries = %d, want 5", cfg.HealthCheck.Retries)
	}
	if cfg.Logging.Rotation.Compress != false {
		t.Error("Compress should be false")
	}
	// Defaults should still apply for unset fields
	if cfg.Daemon.PIDFile != "/var/run/dpm/dpm.pid" {
		t.Errorf("PIDFile = %s, want default", cfg.Daemon.PIDFile)
	}
}

func TestLoadDaemonConfigNotFound(t *testing.T) {
	_, err := LoadDaemonConfig("/nonexistent/config.yaml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadProcessConfig(t *testing.T) {
	content := `
type: nodejs
name: my-app
command: npm run start
cwd: /home/depfloy/42/current
instances: 2
port: auto
env:
  NODE_ENV: production
health_check:
  type: http
  path: /
  interval: 10s
resources:
  max_memory: 512MB
`
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	os.WriteFile(path, []byte(content), 0644)

	cfg, err := LoadProcessConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Name != "my-app" {
		t.Errorf("Name = %s, want my-app", cfg.Name)
	}
	if cfg.Type != "nodejs" {
		t.Errorf("Type = %s, want nodejs", cfg.Type)
	}
	if cfg.Instances != 2 {
		t.Errorf("Instances = %d, want 2", cfg.Instances)
	}
	if cfg.Port != "auto" {
		t.Errorf("Port = %s, want auto", cfg.Port)
	}
	if cfg.Env["NODE_ENV"] != "production" {
		t.Errorf("NODE_ENV = %s, want production", cfg.Env["NODE_ENV"])
	}
	if cfg.HealthCheck == nil {
		t.Fatal("HealthCheck is nil")
	}
	if cfg.HealthCheck.Type != "http" {
		t.Errorf("HealthCheck.Type = %s, want http", cfg.HealthCheck.Type)
	}
	if cfg.Resources.MaxMemory != "512MB" {
		t.Errorf("MaxMemory = %s, want 512MB", cfg.Resources.MaxMemory)
	}
	// Default restart policy
	if cfg.RestartPolicy != "always" {
		t.Errorf("RestartPolicy = %s, want always", cfg.RestartPolicy)
	}
}

func TestParseProcessConfigJSON(t *testing.T) {
	jsonData := []byte(`{
		"type": "worker",
		"name": "horizon",
		"command": "php artisan horizon",
		"cwd": "/home/depfloy/43/current",
		"restart_policy": "on-failure",
		"max_restarts": 10
	}`)

	cfg, err := ParseProcessConfigJSON(jsonData)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.Name != "horizon" {
		t.Errorf("Name = %s, want horizon", cfg.Name)
	}
	if cfg.RestartPolicy != "on-failure" {
		t.Errorf("RestartPolicy = %s, want on-failure", cfg.RestartPolicy)
	}
	if cfg.MaxRestarts != 10 {
		t.Errorf("MaxRestarts = %d, want 10", cfg.MaxRestarts)
	}
	if cfg.Instances != 1 {
		t.Errorf("Instances = %d, want 1 (default)", cfg.Instances)
	}
}

func TestProcessConfigWithWorkers(t *testing.T) {
	content := `
type: php
name: my-laravel
cwd: /home/depfloy/43/current
command: ""
workers:
  - name: horizon
    command: php artisan horizon
    restart_on_deploy: true
  - name: reverb
    command: php artisan reverb:start --host=127.0.0.1 --port=auto
    port: auto
    nginx:
      websocket: true
      path: /app
`
	dir := t.TempDir()
	path := filepath.Join(dir, "php.yaml")
	os.WriteFile(path, []byte(content), 0644)

	cfg, err := LoadProcessConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(cfg.Workers) != 2 {
		t.Fatalf("Workers = %d, want 2", len(cfg.Workers))
	}
	if cfg.Workers[0].Name != "horizon" {
		t.Errorf("Worker[0].Name = %s, want horizon", cfg.Workers[0].Name)
	}
	if !cfg.Workers[0].RestartOnDeploy {
		t.Error("Worker[0].RestartOnDeploy should be true")
	}
	if cfg.Workers[1].Port != "auto" {
		t.Errorf("Worker[1].Port = %s, want auto", cfg.Workers[1].Port)
	}
	if !cfg.Workers[1].Nginx.WebSocket {
		t.Error("Worker[1].Nginx.WebSocket should be true")
	}
}
