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

# Download binary
BINARY_URL="$BASE_URL/v$VERSION/dpm-$OS-$ARCH"
CHECKSUM_URL="$BASE_URL/v$VERSION/checksums.txt"

TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

echo "==> Downloading binary..."
curl -fsSL -o "$TMP_DIR/dpm" "$BINARY_URL"

echo "==> Downloading checksums..."
curl -fsSL -o "$TMP_DIR/checksums.txt" "$CHECKSUM_URL"

# Verify checksum
echo "==> Verifying checksum..."
EXPECTED=$(grep "dpm-$OS-$ARCH" "$TMP_DIR/checksums.txt" | awk '{print $1}')
ACTUAL=$(sha256sum "$TMP_DIR/dpm" | awk '{print $1}')

if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "Checksum verification failed!"
    echo "  Expected: $EXPECTED"
    echo "  Actual:   $ACTUAL"
    exit 1
fi
echo "    Checksum OK"

# Install binary
echo "==> Installing binary to $PREFIX/bin/"
chmod +x "$TMP_DIR/dpm"

# The same binary serves as both CLI and daemon based on how it's invoked
cp "$TMP_DIR/dpm" "$PREFIX/bin/dpm.new"
mv -f "$PREFIX/bin/dpm.new" "$PREFIX/bin/dpm"

# Create symlink for daemon
ln -sf "$PREFIX/bin/dpm" "$PREFIX/bin/dpmd"

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
