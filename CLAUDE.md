# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What DPM is

DPM (Depfloy Process Manager) is a lightweight, single-binary process manager for production Linux servers, conceptually similar to PM2 but written in Go. It runs as a systemd service and is primarily driven by **Depfloy** (a separate PHP orchestrator), not by humans — many design decisions (explicit port lists, no port allocation, nginx left to Depfloy, blue-green deploy requiring an explicit `drain` step) only make sense in that context. Treat Depfloy as the main API consumer.

## Build, test, lint

```bash
make build        # build bin/dpm + bin/dpmd for current platform
make build-linux  # cross-compile amd64 + arm64 (production targets)
make test         # go test -v -race ./...
make lint         # golangci-lint run ./...
sudo make install # copy binaries to /usr/local/bin
```

Run a single package's tests: `go test -v -race ./internal/process/...`
Run a single test: `go test -v -race ./internal/process/ -run TestName`

Version is injected at build time via ldflags from `git describe --tags` into both `main.version` (CLI) and `internal/daemon.Version` (daemon). CI (`.github/workflows/release.yml`) runs only a subset of tests (`log`, `port`, `state`, `config`) on tag push, then builds and uploads binaries + install script to Cloudflare R2.

## Single-binary, two-entrypoint architecture

The same codebase produces two binaries:
- **`cmd/dpm`** (`main.go`) — the CLI. A thin client: it parses the command, builds an HTTP request, and talks to the daemon over a **Unix domain socket**. It contains no process-management logic. Socket path comes from `$DPM_SOCKET`, default `/var/run/dpm/dpm.sock`.
- **`cmd/dpmd`** (`main.go`) — the daemon. Loads `/etc/dpm/config.yaml`, constructs a `daemon.Daemon`, and runs until SIGTERM/SIGINT.

All CLI↔daemon communication is REST-over-Unix-socket (`/api/v1/...`, see `docs/api.md`). When changing behavior, the change almost always belongs in the daemon/`internal` packages, and the CLI is only updated to add a flag or format output.

## Daemon subsystem wiring

`internal/daemon/daemon.go` is the composition root. `daemon.New()` constructs and wires every subsystem; `Run()` writes the PID file, **adopts orphans**, starts the API server, and blocks on signals. The subsystems (all under `internal/`):

- **`process.Manager`** — process lifecycle. The heart of the system. Tracks running instances in a `map[string]*managed` keyed by `"name"` or `"name:instance"`. Handles spawning, stop signals/timeouts, restart policy with backoff, multi-worker, and the `pendingDrain` map of old workers parked during blue-green deploys.
- **`state.Store`** — BoltDB (`go.etcd.io/bbolt`) persistence at `/var/lib/dpm/dpm.db`, buckets `processes`/`ports`/`meta`. Survives daemon restarts and is the source of truth for orphan adoption. The full `ProcessConfig` is persisted as `config_json` so a restarted daemon can fully reconstruct a process.
- **`port.Manager`** — **validates and tracks** ports only. It does NOT allocate them — Depfloy assigns explicit port lists. Worker count is derived from `len(ports)`, not from a config number.
- **`health.Checker`** — http/tcp/exec health checks with `OnHealthy`/`OnUnhealthy` callbacks. In this codebase the callbacks only log; nginx routing is Depfloy's job.
- **`nginx.Manager`** — renders embedded templates (`internal/nginx/templates/*.tmpl`, via `//go:embed`) and reloads nginx. Driven by `nginx/apply` API calls from Depfloy carrying domains/SSL/PHP version.
- **`internal/api/router.go`** — registers all `/api/v1/*` routes and is the bridge between HTTP handlers and the managers. Every response uses the `Response{status,data,error,meta}` envelope.
- **`log.Engine`** — per-process structured stdout/stderr logging with rotation (`internal/log/rotation.go`).
- **`deploy.Orchestrator`** — zero-downtime deploy logic (symlink swap + rolling/cluster restart).
- **`internal/upgrade`** — self-upgrade / rollback of the DPM binary.

`pkg/config` is the only package under `pkg/` (importable contract). `ProcessConfig` carries helper methods that encode key business rules — `IsClusterMode()`, `ResolveWorkerCount()`, `UpstreamStrategy()`, `DrainTimeout()`. Prefer these helpers over re-deriving the logic. Note one subtlety: `process.Manager.Start` uses `len(ports)` as the actual worker count and only falls back to `ResolveWorkerCount()` when the ports slice is empty — so the explicit port list, not the config number, normally wins.

## Two behaviors that drive most of the design

**Orphan adoption (survive daemon restart).** systemd is configured with `KillMode=process`, so restarting the daemon does NOT kill managed apps. On startup `daemon.adoptOrphans()` reads persisted processes: live PIDs are re-`Attach`ed and monitored via `process.monitorAdopted` (which has no `exec.Cmd` handle, only the PID); dead/errored/stopped processes or those past the restart cap are cleaned from state. Adopted-process monitoring must respect `stopCh` — a `stop` command must not trigger a restart (this has been a recurring bug, see git history).

**Blue-green deploy with explicit drain.** `dpm deploy` starts new workers on new ports while old workers keep serving, waits for new workers to be healthy (auto-rollback on failure), promotes them, and parks the old workers in `pendingDrain`. The old workers are NOT stopped automatically — the caller (Depfloy) repoints nginx and then calls `dpm drain <name>` explicitly. This gives the orchestrator instant rollback by switching nginx back before draining. Do not "simplify" deploy to auto-stop old workers; the two-phase nature is intentional.

## Conventions

- Process types: `nodejs`, `php`, `static`, `worker`. Restart policies: `always`, `on-failure`, `never`. Status constants live in `internal/process/manager.go` (`StatusOnline`, etc.).
- Durations in config are strings (`"10s"`, `"30s"`) parsed at use-site; sizes are strings (`"512MB"`, `"100MB"`).
- `internal/process/manager.go` is heavily concurrent (`sync.RWMutex` over the maps, per-instance `stopCh`). When touching lifecycle code, mind the lock and the channels — most past regressions were races or stop/restart channel mistakes.
- `docs/api.md` documents the REST API; keep it in sync when adding/changing endpoints.
