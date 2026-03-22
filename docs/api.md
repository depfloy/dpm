# DPM API Reference

DPM exposes a REST API over a Unix domain socket. All communication between the `dpm` CLI and the `dpmd` daemon happens through this API.

## Connection

The API listens on a Unix socket at `/var/run/dpm/dpm.sock` (configurable in `/etc/dpm/config.yaml`).

To make requests directly with curl:

```bash
curl --unix-socket /var/run/dpm/dpm.sock http://dpm/api/v1/processes
```

The CLI connects to the socket specified by the `DPM_SOCKET` environment variable, falling back to `/var/run/dpm/dpm.sock`.

## Response Format

All endpoints return a consistent JSON envelope:

```json
{
  "status": "success",
  "data": { },
  "meta": {
    "version": "1.0.0"
  }
}
```

On error:

```json
{
  "status": "error",
  "error": "descriptive error message",
  "meta": {
    "version": "1.0.0"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"success"` or `"error"` |
| `data` | object/array | Response payload (omitted on error) |
| `error` | string | Error message (omitted on success) |
| `meta.version` | string | Daemon version |

---

## Process Endpoints

### List Processes

```
GET /api/v1/processes
```

Returns all managed processes with runtime metrics.

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "name": "my-app",
      "pid": 12345,
      "status": "online",
      "port": 3000,
      "type": "nodejs",
      "memory_bytes": 50593792,
      "cpu_percent": 2.5,
      "uptime_ns": 8100000000000,
      "restart_count": 0,
      "started_at": "2026-03-22T10:00:00Z",
      "command": "node server.js",
      "cwd": "/home/depfloy/my-app/current"
    },
    {
      "name": "my-app:1",
      "pid": 12346,
      "status": "online",
      "port": 3001,
      "type": "nodejs",
      "memory_bytes": 48234496,
      "cpu_percent": 1.8,
      "uptime_ns": 8100000000000,
      "restart_count": 0,
      "started_at": "2026-03-22T10:00:00Z",
      "command": "node server.js",
      "cwd": "/home/depfloy/my-app/current"
    }
  ],
  "meta": { "version": "1.0.0" }
}
```

Process status values: `online`, `stopped`, `starting`, `stopping`, `errored`.

For multi-instance processes, each instance is listed separately with the naming convention `name:index` (e.g., `my-app:0`, `my-app:1`). Single-instance processes use only the name.

---

### Create Process

```
POST /api/v1/processes
```

Creates and starts a new managed process. The request body is a JSON-encoded process configuration.

**Request body:**

```json
{
  "name": "my-app",
  "type": "nodejs",
  "command": "node server.js",
  "cwd": "/home/depfloy/my-app/current",
  "port": "auto",
  "instances": 2,
  "env": {
    "NODE_ENV": "production"
  },
  "health_check": {
    "type": "http",
    "path": "/health",
    "interval": "10s",
    "timeout": "5s",
    "healthy_threshold": 2,
    "unhealthy_threshold": 3
  },
  "restart_policy": "always",
  "max_restarts": 10,
  "stop_signal": "SIGTERM",
  "stop_timeout": "10s",
  "resources": {
    "max_memory": "512MB",
    "max_cpu": 2
  }
}
```

Required fields: `name`, `command`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | (required) | Unique process identifier |
| `type` | string | | Process type: `nodejs`, `php`, `static`, `worker` |
| `command` | string | (required) | Shell command to execute |
| `cwd` | string | | Working directory for the process |
| `port` | string | | `"auto"` for automatic allocation, or a specific port number |
| `instances` | int | `1` | Number of instances to run (legacy mode) |
| `cluster` | object | | Cluster mode configuration (see below) |
| `env` | object | | Environment variables as key-value pairs |
| `health_check` | object | | Health check configuration (see below) |
| `restart_policy` | string | `"always"` | `always`, `on-failure`, `never` |
| `restart_delay` | string | | Delay before restart (e.g., `"1s"`) |
| `max_restarts` | int | `0` (unlimited) | Maximum restart attempts |
| `stop_signal` | string | `"SIGTERM"` | Signal to send on stop: `SIGTERM`, `SIGKILL`, `SIGINT`, `SIGQUIT` |
| `stop_timeout` | string | `"10s"` | Time to wait for graceful shutdown before SIGKILL |
| `resources` | object | | Resource limits |
| `nginx` | object | | Nginx proxy configuration |
| `workers` | array | | Sub-worker processes attached to this process |

**Cluster configuration:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | | `auto` (CPU cores - 1, min 2) or `fixed` |
| `workers` | int | `2` | Worker count when mode is `fixed` |
| `strategy` | string | `"least_conn"` | Upstream strategy: `least_conn`, `round_robin`, `ip_hash` |
| `drain_timeout` | string | `"30s"` | Connection drain timeout during shutdown |

**Health check configuration:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | | `http`, `tcp`, or `exec` |
| `path` | string | `"/"` | HTTP path for health check (http type only) |
| `command` | string | | Command to execute (exec type only) |
| `interval` | string | `"10s"` | Time between checks |
| `timeout` | string | `"5s"` | Check timeout |
| `healthy_threshold` | int | `2` | Consecutive passes to be considered healthy |
| `unhealthy_threshold` | int | `3` | Consecutive failures to be considered unhealthy |

**Response (201):**

```json
{
  "status": "success",
  "data": {
    "name": "my-app",
    "ports": [3000, 3001]
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `400` -- Invalid config, missing required fields, or invalid port value
- `409` -- Requested port is already allocated to another process
- `500` -- Port allocation or process start failure

---

### Get Process Details

```
GET /api/v1/processes/{name}
```

Returns detailed information about a specific process, including all instances and health status.

**Response:**

```json
{
  "status": "success",
  "data": {
    "instances": [
      {
        "name": "my-app",
        "pid": 12345,
        "status": "online",
        "port": 3000,
        "type": "nodejs",
        "memory_bytes": 50593792,
        "cpu_percent": 2.5,
        "uptime_ns": 8100000000000,
        "restart_count": 0,
        "started_at": "2026-03-22T10:00:00Z",
        "command": "node server.js",
        "cwd": "/home/depfloy/my-app/current",
        "env": {
          "NODE_ENV": "production"
        }
      }
    ],
    "health": {
      "healthy": true,
      "check_type": "http",
      "message": "HTTP 200",
      "response_time_ns": 1500000,
      "last_check": "2026-03-22T12:15:00Z",
      "consecutive": 5
    }
  },
  "meta": { "version": "1.0.0" }
}
```

The `health` field is `null` if no health check is configured for the process.

**Error responses:**

- `404` -- Process not found

---

### Delete Process

```
DELETE /api/v1/processes/{name}
```

Stops all instances of the process, releases allocated ports, stops health monitoring, and removes the process from management.

**Response:**

```json
{
  "status": "success",
  "data": {
    "deleted": "my-app"
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `404` -- Process not found

---

### Start Process

```
POST /api/v1/processes/{name}/start
```

Starts a previously stopped process using its stored configuration and port assignments.

**Response:**

```json
{
  "status": "success",
  "data": {
    "started": "my-app"
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `404` -- Process not found
- `500` -- Process failed to start

---

### Stop Process

```
POST /api/v1/processes/{name}/stop
```

Sends the configured stop signal (default SIGTERM) to all instances. Waits for the configured `stop_timeout` before sending SIGKILL.

**Response:**

```json
{
  "status": "success",
  "data": {
    "stopped": "my-app"
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `404` -- Process not found

---

### Restart Process

```
POST /api/v1/processes/{name}/restart
```

Stops all instances, waits briefly for ports to free, then starts the process again with the same configuration and port assignments.

**Response:**

```json
{
  "status": "success",
  "data": {
    "restarted": "my-app"
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `500` -- Restart failed

---

## Port Endpoints

### List Port Allocations

```
GET /api/v1/ports
```

Returns all currently allocated ports.

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "port": 3000,
      "process_name": "my-app",
      "type": "nodejs",
      "allocated_at": "2026-03-22T10:00:00Z"
    },
    {
      "port": 3001,
      "process_name": "my-app",
      "type": "nodejs",
      "allocated_at": "2026-03-22T10:00:00Z"
    },
    {
      "port": 5000,
      "process_name": "reverb",
      "type": "plugins",
      "allocated_at": "2026-03-22T09:00:00Z"
    }
  ],
  "meta": { "version": "1.0.0" }
}
```

Port ranges by process type:

| Type | Range |
|------|-------|
| `nodejs` | 3000 - 4999 |
| `plugins` / `php` | 5000 - 5999 |
| `worker` | 6000 - 6999 |

---

### Allocate Ports

```
POST /api/v1/ports/allocate
```

Manually allocate ports from the appropriate range. Ports are verified to be free before allocation.

**Request body:**

```json
{
  "process_name": "my-service",
  "type": "nodejs",
  "count": 2
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `process_name` | string | | Name to associate with the allocation |
| `type` | string | | Port range to allocate from: `nodejs`, `plugins`, `worker` |
| `count` | int | `1` | Number of ports to allocate |

**Response:**

```json
{
  "status": "success",
  "data": {
    "ports": [3002, 3003]
  },
  "meta": { "version": "1.0.0" }
}
```

**Error responses:**

- `400` -- Invalid request body
- `500` -- Not enough free ports in range

---

## System Endpoints

### Daemon Status

```
GET /api/v1/status
```

Returns an overview of the daemon state.

**Response:**

```json
{
  "status": "success",
  "data": {
    "processes_total": 5,
    "processes_online": 4,
    "ports_allocated": 8
  },
  "meta": { "version": "1.0.0" }
}
```

---

### Health Check

```
GET /api/v1/health
```

Returns health status for all processes and their health check results.

**Response:**

```json
{
  "status": "success",
  "data": {
    "healthy": true,
    "processes": [
      {
        "name": "my-app",
        "pid": 12345,
        "status": "online",
        "port": 3000,
        "type": "nodejs",
        "memory_bytes": 50593792,
        "cpu_percent": 2.5,
        "uptime_ns": 8100000000000,
        "restart_count": 0,
        "started_at": "2026-03-22T10:00:00Z",
        "command": "node server.js",
        "cwd": "/home/depfloy/my-app/current"
      }
    ],
    "checks": {
      "my-app": {
        "healthy": true,
        "check_type": "http",
        "message": "HTTP 200",
        "response_time_ns": 1500000,
        "last_check": "2026-03-22T12:15:00Z",
        "consecutive": 5
      }
    }
  },
  "meta": { "version": "1.0.0" }
}
```

The top-level `healthy` field is `false` if any process is in a non-`online`/non-`stopped` state or if any health check reports unhealthy.

---

### Version

```
GET /api/v1/version
```

Returns the daemon version.

**Response:**

```json
{
  "status": "success",
  "data": {
    "version": "1.0.0"
  },
  "meta": { "version": "1.0.0" }
}
```
