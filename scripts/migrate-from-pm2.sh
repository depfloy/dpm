#!/bin/bash
set -euo pipefail

# PM2 → DPM Migration Script
# Migrates running PM2 processes and Supervisor programs to DPM.
#
# Usage: bash migrate-from-pm2.sh [options]
#   --dry-run       Show what would be done without making changes
#   --keep-pm2      Don't remove PM2 after migration
#   --keep-supervisor  Don't stop Supervisor after migration
#   --skip-pm2      Skip PM2 migration
#   --skip-supervisor  Skip Supervisor migration

DRY_RUN=false
KEEP_PM2=false
KEEP_SUPERVISOR=false
SKIP_PM2=false
SKIP_SUPERVISOR=false

for arg in "$@"; do
    case $arg in
        --dry-run)         DRY_RUN=true ;;
        --keep-pm2)        KEEP_PM2=true ;;
        --keep-supervisor) KEEP_SUPERVISOR=true ;;
        --skip-pm2)        SKIP_PM2=true ;;
        --skip-supervisor) SKIP_SUPERVISOR=true ;;
        --help)
            echo "Usage: migrate-from-pm2.sh [--dry-run] [--keep-pm2] [--keep-supervisor] [--skip-pm2] [--skip-supervisor]"
            exit 0
            ;;
    esac
done

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ── 0. Pre-flight checks ──────────────────────────────────────────────────

if ! command -v dpm &>/dev/null; then
    log_error "DPM is not installed. Install it first:"
    echo "  curl -fsSL https://get.depfloy.com/dpm/install.sh | bash"
    exit 1
fi

DPM_VERSION=$(dpm version --short 2>/dev/null || echo "unknown")
log_info "DPM version: $DPM_VERSION"

if $DRY_RUN; then
    log_warn "DRY RUN MODE - no changes will be made"
    echo ""
fi

MIGRATED_PM2=0
MIGRATED_SUPERVISOR=0
FAILED=0

# ── 1. Migrate PM2 processes ──────────────────────────────────────────────

if ! $SKIP_PM2 && command -v pm2 &>/dev/null; then
    log_info "=== Migrating PM2 processes ==="

    PM2_JSON=$(pm2 jlist 2>/dev/null || echo "[]")
    PM2_COUNT=$(echo "$PM2_JSON" | jq 'length' 2>/dev/null || echo "0")

    if [ "$PM2_COUNT" -eq 0 ]; then
        log_info "No PM2 processes found"
    else
        log_info "Found $PM2_COUNT PM2 process(es)"

        echo "$PM2_JSON" | jq -c '.[]' | while read -r proc; do
            PM2_NAME=$(echo "$proc" | jq -r '.name')
            PM2_PID=$(echo "$proc" | jq -r '.pid')
            PM2_STATUS=$(echo "$proc" | jq -r '.pm2_env.status')
            PM2_CWD=$(echo "$proc" | jq -r '.pm2_env.pm_cwd // .pm2_env.cwd // "/"')
            PM2_EXEC_PATH=$(echo "$proc" | jq -r '.pm2_env.pm_exec_path // ""')
            PM2_SCRIPT=$(echo "$proc" | jq -r '.pm2_env.args // [] | join(" ")')
            PM2_INTERPRETER=$(echo "$proc" | jq -r '.pm2_env.exec_interpreter // "node"')
            PM2_PORT=$(echo "$proc" | jq -r '.pm2_env.env.PORT // empty' 2>/dev/null || echo "")
            PM2_NITRO_PORT=$(echo "$proc" | jq -r '.pm2_env.env.NITRO_PORT // empty' 2>/dev/null || echo "")
            PM2_MAX_MEMORY=$(echo "$proc" | jq -r '.pm2_env.max_memory_restart // 0')

            # Build command
            COMMAND=""
            if [ -n "$PM2_EXEC_PATH" ]; then
                if [ "$PM2_INTERPRETER" = "none" ] || [ "$PM2_INTERPRETER" = "node" ]; then
                    COMMAND="$PM2_EXEC_PATH"
                else
                    COMMAND="$PM2_INTERPRETER $PM2_EXEC_PATH"
                fi
                if [ -n "$PM2_SCRIPT" ]; then
                    COMMAND="$COMMAND $PM2_SCRIPT"
                fi
            fi

            # Determine port
            PORT=""
            if [ -n "$PM2_PORT" ]; then
                PORT="$PM2_PORT"
            elif [ -n "$PM2_NITRO_PORT" ]; then
                PORT="$PM2_NITRO_PORT"
            fi

            # Determine max memory
            MAX_MEM=""
            if [ "$PM2_MAX_MEMORY" -gt 0 ] 2>/dev/null; then
                MAX_MEM_MB=$((PM2_MAX_MEMORY / 1048576))
                MAX_MEM="${MAX_MEM_MB}MB"
            fi

            echo ""
            log_info "  Process: $PM2_NAME (PID: $PM2_PID, Status: $PM2_STATUS)"
            log_info "    Command: $COMMAND"
            log_info "    CWD: $PM2_CWD"
            [ -n "$PORT" ] && log_info "    Port: $PORT"

            if $DRY_RUN; then
                log_warn "    [dry-run] Would create DPM process '$PM2_NAME'"
                MIGRATED_PM2=$((MIGRATED_PM2 + 1))
                continue
            fi

            # Build DPM config
            DPM_CONFIG=$(jq -n \
                --arg name "$PM2_NAME" \
                --arg command "$COMMAND" \
                --arg cwd "$PM2_CWD" \
                --arg port "$PORT" \
                --arg max_memory "$MAX_MEM" \
                '{
                    type: "worker",
                    name: $name,
                    command: $command,
                    cwd: $cwd,
                    restart_policy: "always",
                    port: (if $port != "" then $port else null end),
                    resources: (if $max_memory != "" then {max_memory: $max_memory} else null end)
                } | del(.[] | nulls)')

            # Stop PM2 process
            pm2 stop "$PM2_NAME" 2>/dev/null || true

            # Start via DPM
            ESCAPED_CONFIG=$(echo "$DPM_CONFIG" | jq -c '.')
            if dpm start --config="$ESCAPED_CONFIG" 2>&1; then
                log_info "    ✓ Migrated to DPM"
                pm2 delete "$PM2_NAME" 2>/dev/null || true
                MIGRATED_PM2=$((MIGRATED_PM2 + 1))
            else
                log_error "    ✗ Failed to start in DPM, restarting in PM2"
                pm2 start "$PM2_NAME" 2>/dev/null || true
                FAILED=$((FAILED + 1))
            fi
        done
    fi

    if ! $KEEP_PM2 && ! $DRY_RUN && [ "$FAILED" -eq 0 ]; then
        log_info "Cleaning up PM2..."
        pm2 kill 2>/dev/null || true
        # Don't uninstall PM2 automatically - user may want to keep it
        log_info "PM2 daemon stopped. Run 'npm uninstall -g pm2' to fully remove."
    fi

elif ! $SKIP_PM2; then
    log_info "PM2 not found, skipping"
fi

# ── 2. Migrate Supervisor programs ────────────────────────────────────────

if ! $SKIP_SUPERVISOR && [ -d /etc/supervisor/conf.d ]; then
    log_info ""
    log_info "=== Migrating Supervisor programs ==="

    SUPERVISOR_CONFIGS=$(ls /etc/supervisor/conf.d/depfloy-*.conf 2>/dev/null || true)

    if [ -z "$SUPERVISOR_CONFIGS" ]; then
        log_info "No Depfloy Supervisor programs found"
    else
        for config_file in $SUPERVISOR_CONFIGS; do
            PROGRAM_NAME=$(grep -oP '(?<=\[program:)[^\]]+' "$config_file" 2>/dev/null || continue)
            COMMAND=$(grep -oP '(?<=command=).+' "$config_file" 2>/dev/null || echo "")
            DIRECTORY=$(grep -oP '(?<=directory=).+' "$config_file" 2>/dev/null || echo "/")
            USER=$(grep -oP '(?<=user=).+' "$config_file" 2>/dev/null || echo "depfloy")
            NUMPROCS=$(grep -oP '(?<=numprocs=)\d+' "$config_file" 2>/dev/null || echo "1")
            AUTORESTART=$(grep -oP '(?<=autorestart=)\w+' "$config_file" 2>/dev/null || echo "true")
            STOPSIGNAL=$(grep -oP '(?<=stopsignal=)\w+' "$config_file" 2>/dev/null || echo "SIGTERM")
            STOPWAITSECS=$(grep -oP '(?<=stopwaitsecs=)\d+' "$config_file" 2>/dev/null || echo "10")

            RESTART_POLICY="always"
            if [ "$AUTORESTART" = "false" ]; then
                RESTART_POLICY="never"
            fi

            echo ""
            log_info "  Program: $PROGRAM_NAME"
            log_info "    Command: $COMMAND"
            log_info "    Directory: $DIRECTORY"
            log_info "    User: $USER"

            if $DRY_RUN; then
                log_warn "    [dry-run] Would create DPM process '$PROGRAM_NAME'"
                MIGRATED_SUPERVISOR=$((MIGRATED_SUPERVISOR + 1))
                continue
            fi

            # Build DPM config
            DPM_CONFIG=$(jq -n \
                --arg name "$PROGRAM_NAME" \
                --arg command "$COMMAND" \
                --arg cwd "$DIRECTORY" \
                --arg user "$USER" \
                --argjson instances "$NUMPROCS" \
                --arg restart_policy "$RESTART_POLICY" \
                --arg stop_signal "$STOPSIGNAL" \
                --arg stop_timeout "${STOPWAITSECS}s" \
                '{
                    type: "worker",
                    name: $name,
                    command: $command,
                    cwd: $cwd,
                    user: $user,
                    instances: $instances,
                    restart_policy: $restart_policy,
                    stop_signal: $stop_signal,
                    stop_timeout: $stop_timeout
                }')

            # Stop supervisor program
            sudo supervisorctl stop "$PROGRAM_NAME" 2>/dev/null || true

            # Start via DPM
            ESCAPED_CONFIG=$(echo "$DPM_CONFIG" | jq -c '.')
            if dpm start --config="$ESCAPED_CONFIG" 2>&1; then
                log_info "    ✓ Migrated to DPM"
                # Remove supervisor config
                sudo rm -f "$config_file"
                MIGRATED_SUPERVISOR=$((MIGRATED_SUPERVISOR + 1))
            else
                log_error "    ✗ Failed to start in DPM, restarting in Supervisor"
                sudo supervisorctl start "$PROGRAM_NAME" 2>/dev/null || true
                FAILED=$((FAILED + 1))
            fi
        done

        # Reload supervisor after removing configs
        if ! $DRY_RUN && ! $KEEP_SUPERVISOR; then
            sudo supervisorctl reread 2>/dev/null || true
            sudo supervisorctl update 2>/dev/null || true
        fi
    fi
else
    log_info "Supervisor config dir not found or skipped"
fi

# ── 3. Summary ────────────────────────────────────────────────────────────

echo ""
log_info "=== Migration Summary ==="
log_info "  PM2 processes migrated:        $MIGRATED_PM2"
log_info "  Supervisor programs migrated:  $MIGRATED_SUPERVISOR"
if [ "$FAILED" -gt 0 ]; then
    log_error "  Failed:                        $FAILED"
fi
echo ""

if $DRY_RUN; then
    log_warn "This was a dry run. Run without --dry-run to apply changes."
elif [ "$FAILED" -eq 0 ]; then
    log_info "Migration complete! Verify with: dpm list"
else
    log_error "Some migrations failed. Check the output above."
    exit 1
fi
