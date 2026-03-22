#!/bin/bash
# DPM Upgrade Watchdog
# Runs after a DPM upgrade to verify the new version is healthy.
# If the daemon doesn't become healthy within TIMEOUT seconds,
# automatically rolls back to the previous binary.

TIMEOUT=${1:-30}
STARTED=$(date +%s)

while true; do
    ELAPSED=$(( $(date +%s) - STARTED ))
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "DPM upgrade failed: daemon not healthy after ${TIMEOUT}s"

        # Rollback: restore previous binary
        if [ -f /usr/local/bin/dpm.bak ]; then
            echo "Rolling back to previous version..."
            mv -f /usr/local/bin/dpm.bak /usr/local/bin/dpm
            ln -sf /usr/local/bin/dpm /usr/local/bin/dpmd
            systemctl restart dpm

            sleep 3
            if dpm health --json 2>/dev/null | grep -q '"healthy":true'; then
                echo "Rollback successful"
            else
                echo "WARNING: Rollback may have failed, check manually: systemctl status dpm"
            fi
        else
            echo "No backup binary found, cannot rollback"
        fi
        exit 1
    fi

    # Check daemon health
    if dpm health --json 2>/dev/null | grep -q '"healthy":true'; then
        echo "DPM upgrade successful: v$(dpm version --short 2>/dev/null || echo 'unknown')"
        # Clean up backup
        rm -f /usr/local/bin/dpm.bak
        exit 0
    fi

    sleep 2
done
