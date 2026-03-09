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
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    golang-go \
    unzip

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

# Install Docker Engine
if ! command -v docker &>/dev/null; then
    echo "Installing Docker Engine..."
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io
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

# Install Supabase CLI
if ! command -v supabase &>/dev/null; then
    echo "Installing Supabase CLI..."
    ARCH=$(dpkg --print-architecture)
    SUPA_VER=$(curl -fsSL https://api.github.com/repos/supabase/cli/releases/latest | grep -oP '"tag_name":\s*"v\K[^"]+')
    curl -fsSL "https://github.com/supabase/cli/releases/download/v${SUPA_VER}/supabase_${SUPA_VER}_linux_${ARCH}.deb" -o /tmp/supabase.deb
    sudo dpkg -i /tmp/supabase.deb
    rm -f /tmp/supabase.deb
fi

# Install Playwright browsers
if ! npx playwright --version &>/dev/null 2>&1; then
    echo "Installing Playwright browsers..."
    npx playwright install --with-deps chromium
fi

# Create orcest user (if not exists)
if ! id -u orcest &>/dev/null; then
    echo "Creating orcest user..."
    sudo useradd --system --create-home --shell /bin/bash orcest
fi

# Add orcest to docker group (if docker is installed)
if command -v docker &>/dev/null; then
    sudo usermod -aG docker orcest 2>/dev/null || true
fi

# Create workspace directory
WORKSPACE_DIR="/opt/orcest"
sudo mkdir -p "$WORKSPACE_DIR"
sudo mkdir -p "$WORKSPACE_DIR/workspaces"
sudo chown -R orcest:orcest "$WORKSPACE_DIR"

# Create virtualenv for orcest
echo "Creating virtualenv at $WORKSPACE_DIR/venv..."
sudo -u orcest python3 -m venv "$WORKSPACE_DIR/venv"

# Install orcest package (wheel is uploaded separately by `orcest provision`)
WHEEL=$(find /tmp/orcest-wheel/ -name '*.whl' 2>/dev/null | head -1)
if [[ -n "$WHEEL" ]]; then
    echo "Installing orcest from wheel: $(basename "$WHEEL")"
    sudo -u orcest "$WORKSPACE_DIR/venv/bin/pip" install "$WHEEL"
else
    echo "No wheel found at /tmp/orcest-wheel/ — skipping orcest install."
    echo "The provision command will install it in the next step."
fi

# Verify dependencies
echo ""
echo "Verifying installation..."
for cmd in python3 node claude gh git docker go; do
    if command -v "$cmd" &>/dev/null; then
        echo "  $cmd: ok"
    else
        echo "  $cmd: MISSING"
        exit 1
    fi
done
ORCEST_BIN="$WORKSPACE_DIR/venv/bin/orcest"
if [[ -x "$ORCEST_BIN" ]]; then
    echo "  orcest: ok"
else
    echo "  orcest: MISSING"
    exit 1
fi

echo ""
echo "=== Setup complete ==="
