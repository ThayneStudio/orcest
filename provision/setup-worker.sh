#!/usr/bin/env bash
# Setup script for orcest worker VMs.
# Run on the target VM to install dependencies and configure the worker service.
# Tested on Ubuntu 24.04 (Noble) cloud images.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "=== Orcest Worker Setup ==="

# Always run apt-get update first
echo "Updating package lists..."
sudo apt-get update -qq

# Install all apt packages in one shot (idempotent — apt skips already-installed)
echo "Installing system packages..."
sudo apt-get install -y -qq \
    python3 \
    python3-pip \
    python3-venv \
    git \
    curl

# Install Node.js (required for Claude CLI)
if ! command -v node &>/dev/null; then
    echo "Installing Node.js 20.x..."
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y -qq nodejs
fi

# Install Claude CLI
if ! command -v claude &>/dev/null; then
    echo "Installing Claude CLI..."
    sudo npm install -g @anthropic-ai/claude-code
fi

# Install gh CLI
if ! command -v gh &>/dev/null; then
    echo "Installing gh CLI..."
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq gh
fi

# Create orcest user (if not exists)
if ! id -u orcest &>/dev/null; then
    echo "Creating orcest user..."
    sudo useradd --system --create-home --shell /bin/bash orcest
fi

# Create workspace directory
WORKSPACE_DIR="/opt/orcest"
sudo mkdir -p "$WORKSPACE_DIR"
sudo mkdir -p "$WORKSPACE_DIR/workspaces"
sudo chown -R orcest:orcest "$WORKSPACE_DIR"

# Install orcest package (wheel is uploaded separately by `orcest provision`)
WHEEL=$(find /tmp/orcest-wheel/ -name '*.whl' 2>/dev/null | head -1)
if [[ -n "$WHEEL" ]]; then
    echo "Installing orcest from wheel: $(basename "$WHEEL")"
    sudo python3 -m pip install --break-system-packages "$WHEEL"
else
    echo "No wheel found at /tmp/orcest-wheel/ — skipping orcest install."
    echo "The provision command will install it in the next step."
fi

# Verify dependencies
echo ""
echo "Verifying installation..."
for cmd in python3 node claude gh git orcest; do
    if command -v "$cmd" &>/dev/null; then
        echo "  $cmd: ok"
    else
        echo "  $cmd: MISSING"
        exit 1
    fi
done

echo ""
echo "=== Setup complete ==="
