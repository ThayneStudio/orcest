#!/usr/bin/env bash
# Setup script for orcest worker VMs.
# Run on the target VM to install dependencies and configure the worker service.
set -euo pipefail

echo "=== Orcest Worker Setup ==="

# Install Python 3.12+
if ! command -v python3.12 &>/dev/null; then
    echo "Installing Python 3.12..."
    sudo apt-get update
    sudo apt-get install -y python3.12 python3.12-venv python3-pip
fi

# Install gh CLI
if ! command -v gh &>/dev/null; then
    echo "Installing gh CLI..."
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    sudo apt-get update
    sudo apt-get install -y gh
fi

# Install git
if ! command -v git &>/dev/null; then
    sudo apt-get install -y git
fi

# Create workspace directory
WORKSPACE_DIR="/opt/orcest"
sudo mkdir -p "$WORKSPACE_DIR"
sudo chown "$(whoami)" "$WORKSPACE_DIR"

echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Install orcest: pip install orcest"
echo "  2. Copy config/worker.example.yaml to $WORKSPACE_DIR/worker.yaml and edit"
echo "  3. Install systemd service: sudo cp provision/systemd/orcest-worker.service /etc/systemd/system/"
echo "  4. Enable and start: sudo systemctl enable --now orcest-worker"
