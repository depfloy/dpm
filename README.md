# DPM - Depfloy Process Manager

DPM is a lightweight process manager built for production Linux servers. It manages long-running application processes (Node.js, workers, plugins) with health checking, multi-worker support, and zero-downtime blue-green deployments. DPM runs as a systemd service and exposes a Unix socket API that the `dpm` CLI consumes.

## Key Features

- **Process lifecycle management** -- start, stop, restart, and delete processes with automatic restart policies and exponential backoff
- **Multi-worker support** -- run multiple workers per process on explicit ports, with nginx upstream load balancing (least_conn, round_robin, ip_hash)
- **Rolling restart** -- restarts workers one at a time while others continue serving traffic; new ports are started first, then existing ports are cycled sequentially for true zero-downtime
- **Blue-green deployments** -- start new workers on different ports, verify health, then drain old workers with configurable timeout
- **Health checks** -- HTTP, TCP, or exec-based checks with configurable thresholds and intervals
- **Connection draining** -- graceful shutdown waits for active requests to complete before stopping workers
- **Persistent state** -- BoltDB-backed state survives daemon restarts; orphan processes are re-adopted automatically
- **Log management** -- per-process stdout/stderr log files with rotation (size, age, compression)
- **Resource limits** -- configurable memory and CPU constraints per process
- **Single binary** -- the same binary serves as both the CLI (`dpm`) and the daemon (`dpmd`)

## Installation

```bash
curl -fsSL https://get.depfloy.com/dpm/install.sh | bash
```

Install a specific version:

```bash
curl -fsSL https://get.depfloy.com/dpm/install.sh | bash -s -- --version=1.2.0
```

The installer downloads the binary, verifies its SHA-256 checksum, creates the systemd service, and starts the daemon. It supports `amd64` and `arm64` Linux architectures.

After installation:

| Path | Purpose |
|------|---------|
| `/usr/local/bin/dpm` | CLI binary |
| `/usr/local/bin/dpmd` | Daemon symlink |
| `/etc/dpm/config.yaml` | Daemon configuration |
| `/var/log/dpm/` | Daemon and application logs |
| `/var/lib/dpm/` | Persistent state (BoltDB) |
| `/var/run/dpm/dpm.sock` | Unix socket |

## Quick Start

1. Create a process config file `app.yaml`:

```yaml
name: my-app
type: nodejs
command: node server.js
cwd: /home/depfloy/my-app/current
ports: [3000, 3001]

env:
  NODE_ENV: production

health_check:
  type: http
  path: /health
  interval: 10s
  timeout: 5s

restart_policy: always
max_restarts: 10
```

2. Start the process:

```bash
dpm start app.yaml
```

3. Check running processes:

```bash
dpm list
```

Output:

```
NAME      TYPE    STATUS  PID    PORT  MEMORY    UPTIME  RESTARTS
my-app    nodejs  online  12345  3000  48.2 MB   2h 15m  0
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `dpm start <config.yaml>` | Start a new process from a YAML config file |
| `dpm start --config='<json>'` | Start a new process from inline JSON |
| `dpm stop <name>` | Stop a running process and all its instances |
| `dpm restart <name>` | Stop and restart a process, preserving port assignments |
| `dpm deploy --config='<json>'` | Blue-green deploy: start new workers on new ports while old keep serving |
| `dpm drain <name>` | Stop old workers parked during blue-green deploy |
| `dpm delete <name>` | Stop and remove a process from management |
| `dpm list` | List all managed processes in a table |
| `dpm list --json` | List all managed processes as JSON |
| `dpm info <name>` | Show detailed information about a process |
| `dpm status` | Show daemon status (total/online processes) |
| `dpm health` | Check health of all processes |
| `dpm health --json` | Health check output as JSON |
| `dpm port list` | List all port allocations |
| `dpm port release <port>` | Release a port allocation |
| `dpm upgrade --version=<ver>` | Upgrade DPM to a specific version |
| `dpm upgrade --rollback` | Roll back to the previous DPM binary |
| `dpm version` | Show CLI and daemon version |
| `dpm version --short` | Print version number only (no prefix, no daemon check) |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DPM_SOCKET` | `/var/run/dpm/dpm.sock` | Unix socket path for CLI-to-daemon communication |

## Configuration

### Daemon Configuration (`/etc/dpm/config.yaml`)

```yaml
daemon:
  socket: /var/run/dpm/dpm.sock
  pid_file: /var/run/dpm/dpm.pid
  log_file: /var/log/dpm/daemon.log

user: depfloy

logging:
  format: json
  dir: /var/log/dpm
  rotation:
    max_size: 100MB
    max_age: 30d
    max_backups: 10
    compress: true

nginx:
  mode: external           # "external" or "managed"
  config_dir: /etc/nginx
  reload_command: nginx -t && nginx -s reload

health_check:
  default_interval: 10s
  default_timeout: 5s
  default_retries: 3

state:
  dir: /var/lib/dpm
```

### Process Configuration

A process config can be a YAML file passed to `dpm start` or inline JSON via `--config=`.

```yaml
name: my-api
type: nodejs                    # nodejs, php, static, worker
command: node dist/server.js
cwd: /home/depfloy/my-api/current
ports: [3000, 3001]             # Explicit port list

env:
  NODE_ENV: production
  DATABASE_URL: postgres://...

health_check:
  type: http                    # http, tcp, exec
  path: /health
  interval: 10s
  timeout: 5s
  healthy_threshold: 2
  unhealthy_threshold: 3

resources:
  max_memory: 512MB

restart_policy: always          # always, on-failure, never
restart_delay: 1s
max_restarts: 10
stop_signal: SIGTERM            # SIGTERM, SIGKILL, SIGINT, SIGQUIT
stop_timeout: 10s
```

## Cluster Mode

Cluster mode runs multiple worker instances of the same process behind an nginx upstream, enabling horizontal scaling on a single server.

```yaml
name: my-api
type: nodejs
command: node dist/server.js
cwd: /home/depfloy/my-api/current
ports: [3000, 3001, 3002, 3003]

cluster:
  mode: fixed
  workers: 4
  strategy: least_conn          # least_conn (default), round_robin, ip_hash
  drain_timeout: 30s            # Time to wait for active requests during shutdown
```

### Worker Count Resolution

Worker count is determined by the `ports` array length. Each port gets one worker instance.

| Mode | Behavior |
|------|----------|
| `auto` | CPU cores - 1, minimum 2 workers |
| `fixed` | Uses the `workers` value (defaults to 2 if not set) |

### Load Balancing Strategies

| Strategy | Description |
|----------|-------------|
| `least_conn` | Routes to the worker with the fewest active connections (default in cluster mode) |
| `round_robin` | Distributes requests evenly across workers |
| `ip_hash` | Routes requests from the same IP to the same worker |

### Zero-Downtime Deployments

DPM uses blue-green deployment for zero-downtime:

**Blue-green deploy** (`dpm deploy`) -- starts new workers on different ports while old workers continue serving traffic. Old workers are kept alive until the caller explicitly drains them with `dpm drain`.

1. Start new workers on new ports (old workers still running)
2. Wait for all new workers to come online (max 30s, automatic rollback on failure)
3. Promote new workers, park old workers in pending drain state
4. Caller updates nginx to point to new ports, then calls `dpm drain <name>` to stop old workers

This gives the orchestrator (Depfloy) full control over when old workers are stopped, enabling instant rollback by simply switching nginx back to the old port before draining.

## systemd Service

DPM installs as a systemd service with `KillMode=process`. This means only the daemon process is stopped on `systemctl restart dpm` -- managed application processes continue running and are re-adopted by the new daemon instance.

```bash
# Check daemon status
systemctl status dpm

# Restart the daemon (applications keep running)
systemctl restart dpm

# View daemon logs
journalctl -u dpm -f
```

## Build from Source

Requirements: Go 1.23+

```bash
git clone https://github.com/depfloy/dpm.git
cd dpm

# Build for current platform
make build

# Build for Linux amd64 + arm64
make build-linux

# Run tests
make test

# Install locally
sudo make install
```

Build artifacts are placed in `bin/`. The version is injected at build time from `git describe --tags`.

## License

MIT
