#!/bin/bash
set -euo pipefail

# DPM Installer
# Usage: curl -fsSL https://get.depfloy.com/dpm/install.sh | bash -s -- [options]
#
# Options:
#   --version=VERSION    Install specific version (default: latest stable)
#   --channel=CHANNEL    Release channel: stable, beta (default: stable)
#   --prefix=PATH        Install prefix (default: /usr/local)

VERSION=""
CHANNEL="stable"
PREFIX="/usr/local"
BASE_URL="https://get.depfloy.com/dpm"

# Parse arguments
for arg in "$@"; do
    case $arg in
        --version=*)  VERSION="${arg#*=}" ;;
        --channel=*)  CHANNEL="${arg#*=}" ;;
        --prefix=*)   PREFIX="${arg#*=}" ;;
        --help)
            echo "Usage: install.sh [--version=VERSION] [--channel=CHANNEL] [--prefix=PATH]"
            exit 0
            ;;
    esac
done

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "$OS" != "linux" ]; then
    echo "DPM only supports Linux. Detected: $OS"
    exit 1
fi

echo "==> DPM Installer"
echo "    OS: $OS, Arch: $ARCH, Channel: $CHANNEL"

# Resolve version
if [ -z "$VERSION" ]; then
    echo "==> Fetching latest version from $CHANNEL channel..."
    VERSION=$(curl -fsSL "$BASE_URL/channels/$CHANNEL.json" | grep -o '"version":"[^"]*"' | cut -d'"' -f4)

    if [ -z "$VERSION" ]; then
        echo "Failed to fetch latest version"
        exit 1
    fi
fi

echo "==> Installing DPM v$VERSION"

# Download binaries
CLI_URL="$BASE_URL/v$VERSION/dpm-$OS-$ARCH"
DAEMON_URL="$BASE_URL/v$VERSION/dpmd-$OS-$ARCH"
CHECKSUM_URL="$BASE_URL/v$VERSION/checksums.txt"

TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

echo "==> Downloading CLI binary..."
curl -fsSL -o "$TMP_DIR/dpm" "$CLI_URL"

echo "==> Downloading daemon binary..."
curl -fsSL -o "$TMP_DIR/dpmd" "$DAEMON_URL"

echo "==> Downloading checksums..."
curl -fsSL -o "$TMP_DIR/checksums.txt" "$CHECKSUM_URL"

# Verify checksums
echo "==> Verifying checksums..."
EXPECTED_CLI=$(grep "dpm-$OS-$ARCH" "$TMP_DIR/checksums.txt" | head -1 | awk '{print $1}')
ACTUAL_CLI=$(sha256sum "$TMP_DIR/dpm" | awk '{print $1}')

if [ "$EXPECTED_CLI" != "$ACTUAL_CLI" ]; then
    echo "CLI checksum verification failed!"
    echo "  Expected: $EXPECTED_CLI"
    echo "  Actual:   $ACTUAL_CLI"
    exit 1
fi
echo "    CLI checksum OK"

EXPECTED_DAEMON=$(grep "dpmd-$OS-$ARCH" "$TMP_DIR/checksums.txt" | head -1 | awk '{print $1}')
ACTUAL_DAEMON=$(sha256sum "$TMP_DIR/dpmd" | awk '{print $1}')

if [ "$EXPECTED_DAEMON" != "$ACTUAL_DAEMON" ]; then
    echo "Daemon checksum verification failed!"
    echo "  Expected: $EXPECTED_DAEMON"
    echo "  Actual:   $ACTUAL_DAEMON"
    exit 1
fi
echo "    Daemon checksum OK"

# Install binaries
echo "==> Installing binaries to $PREFIX/bin/"
chmod +x "$TMP_DIR/dpm"
chmod +x "$TMP_DIR/dpmd"

# Atomic install: CLI binary
cp "$TMP_DIR/dpm" "$PREFIX/bin/dpm.new"
mv -f "$PREFIX/bin/dpm.new" "$PREFIX/bin/dpm"

# Atomic install: Daemon binary
cp "$TMP_DIR/dpmd" "$PREFIX/bin/dpmd.new"
mv -f "$PREFIX/bin/dpmd.new" "$PREFIX/bin/dpmd"

# Create directories
echo "==> Creating directories..."
mkdir -p /etc/dpm
mkdir -p /var/log/dpm/apps
mkdir -p /var/lib/dpm
mkdir -p /var/run/dpm

# Create default config if not exists
if [ ! -f /etc/dpm/config.yaml ]; then
    echo "==> Creating default config..."
    cat > /etc/dpm/config.yaml << 'YAML'
daemon:
  socket: /var/run/dpm/dpm.sock
  pid_file: /var/run/dpm/dpm.pid
  log_file: /var/log/dpm/daemon.log

user: depfloy

ports:
  nodejs: [3000, 4999]
  plugins: [5000, 5999]
  workers: [6000, 6999]

logging:
  format: json
  dir: /var/log/dpm
  rotation:
    max_size: 100MB
    max_age: 30d
    max_backups: 10
    compress: true

nginx:
  mode: external
  config_dir: /etc/nginx
  reload_command: nginx -t && nginx -s reload

health_check:
  default_interval: 10s
  default_timeout: 5s
  default_retries: 3

state:
  dir: /var/lib/dpm
YAML
fi

# Install systemd service
echo "==> Installing systemd service..."
cat > /etc/systemd/system/dpm.service << 'SYSTEMD'
[Unit]
Description=Depfloy Process Manager
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/dpmd --config=/etc/dpm/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5
LimitNOFILE=65535

# CRITICAL: Only the daemon process is killed on stop/restart.
# Child processes (managed apps) continue running and are
# re-adopted by the new daemon instance on start.
KillMode=process

[Install]
WantedBy=multi-user.target
SYSTEMD

# Reload systemd and enable service
systemctl daemon-reload
systemctl enable dpm

# Set permissions
chown -R root:root /etc/dpm
chmod 600 /etc/dpm/config.yaml

# Allow depfloy user to access socket
if id "depfloy" &>/dev/null; then
    chown root:depfloy /var/run/dpm
    chmod 775 /var/run/dpm
    chown -R depfloy:depfloy /var/log/dpm/apps
fi

# Start or restart daemon
if systemctl is-active --quiet dpm; then
    echo "==> Restarting DPM daemon..."
    systemctl restart dpm
else
    echo "==> Starting DPM daemon..."
    systemctl start dpm
fi

# Wait for daemon to be ready
echo "==> Waiting for daemon..."
for i in $(seq 1 10); do
    if dpm version &>/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Verify
echo ""
echo "==> DPM v$VERSION installed successfully!"
echo ""
dpm version 2>/dev/null || echo "    (daemon not yet ready, try: dpm version)"
echo ""
echo "    Config:  /etc/dpm/config.yaml"
echo "    Logs:    /var/log/dpm/"
echo "    State:   /var/lib/dpm/"
echo "    Socket:  /var/run/dpm/dpm.sock"
echo "    Service: systemctl status dpm"
