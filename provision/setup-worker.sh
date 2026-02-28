#!/usr/bin/env bash
# Setup script for orcest worker VMs.
# Run on the target VM to install dependencies and configure the worker service.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "=== Orcest Worker Setup ==="

# Install Python 3.12+
if ! command -v python3.12 &>/dev/null; then
    echo "Installing Python 3.12..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3.12 python3.12-venv python3-pip
fi

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

# Install git
if ! command -v git &>/dev/null; then
    sudo apt-get install -y -qq git
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

# Install orcest package
echo "Installing orcest..."
sudo python3 -m pip install --break-system-packages git+https://github.com/ThayneStudio/orcest.git || \
    sudo python3 -m pip install git+https://github.com/ThayneStudio/orcest.git

echo "=== Setup complete ==="
