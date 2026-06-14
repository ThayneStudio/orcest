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

# Grok (xAI Grok Build) CLI — provider "grok".
# ----------------------------------------------------------------
# Execution + auth contract lives worker-side in
# src/orcest/worker/grok_runner.py (GrokRunner). The orchestrator stays
# agnostic; see PROVIDER_REGISTRY in src/orcest/worker/runner.py.
#
# Auth is NOT baked here. GrokRunner.prepare_credential writes the per-task
# SuperGrok OAuth blob to $HOME/.grok/auth.json at run time (Path B), so grok
# authenticates unattended off the subscription — no XAI_API_KEY, no login.
# We only bake the self-contained binary; the credential arrives per task in
# the Task payload.
#
# Pinned version: Grok Build is beta and its streaming-json event schema can
# shift between releases. Bump deliberately and re-validate the GrokRunner
# parsers (tests/worker/test_grok_runner.py) when upgrading.
#
# Adding any future provider: see docs/adding-a-provider.md.
GROK_VERSION="0.1.216"
# Pin the installer's SHA-256 to defend against a compromised CDN / DNS hijack
# executing arbitrary code at bake time. FAIL CLOSED: leave empty and the
# installer is NOT run (curl|bash-as-root with no integrity gate is the
# threat); grok degrades gracefully (grok tasks early-reject with a rebake
# hint). Set GROK_INSTALLER_SHA256=<sha256 of x.ai/cli/install.sh> to enable:
#   curl -fsSL https://x.ai/cli/install.sh | sha256sum
# Either way we download to a file first rather than piping a (possibly
# partial) download straight into bash.
GROK_INSTALLER_SHA256="${GROK_INSTALLER_SHA256:-}"
if ! command -v grok &>/dev/null; then
    echo "Installing Grok CLI ${GROK_VERSION}..."
    _grok_installer="$(mktemp)"
    if curl -fsSL https://x.ai/cli/install.sh -o "${_grok_installer}"; then
        _grok_ok=0
        if [ -z "${GROK_INSTALLER_SHA256}" ]; then
            echo "GROK_INSTALLER_SHA256 unset — SKIPPING grok install (fail-closed)."
            echo "  Set GROK_INSTALLER_SHA256=<sha256 of x.ai/cli/install.sh> to enable."
        elif echo "${GROK_INSTALLER_SHA256}  ${_grok_installer}" | sha256sum -c -; then
            _grok_ok=1
        else
            echo "Grok installer checksum mismatch — skipping install"
        fi
        if [ "${_grok_ok}" = "1" ]; then
            bash "${_grok_installer}" "${GROK_VERSION}" || true
        fi
    fi
    rm -f "${_grok_installer}"
    # The installer symlinks ~/.local/bin/grok -> ~/.grok/downloads/<binary>.
    # The worker runs as a systemd service whose PATH may not include
    # ~/.local/bin, so copy the resolved, self-contained binary to a system
    # path. (Verified: grok needs only the binary + a per-task auth.json; no
    # bundled runtime dir.)
    GROK_BIN="$(readlink -f "${HOME}/.local/bin/grok" 2>/dev/null || true)"
    if [ -n "${GROK_BIN}" ] && [ -x "${GROK_BIN}" ]; then
        sudo cp "${GROK_BIN}" /usr/local/bin/grok
        sudo chmod 755 /usr/local/bin/grok
    fi
fi
if command -v grok &>/dev/null; then
    echo "Grok CLI present: $(grok --version 2>/dev/null | head -1)"
else
    echo "WARNING: Grok CLI install failed — Grok tasks will be cleanly rejected"
    echo "  with a permanent FAILED + rebake instruction until the binary is present."
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
# IMPORTANT: gh must be authenticated with a non-expiring token.
# Use a fine-grained PAT (github_pat_…) or classic PAT (ghp_…), NOT an
# OAuth app token (gho_…).  The worker's systemd unit sets ProtectHome=read-only,
# so gh cannot write a refreshed token back to ~/.config/gh/hosts.yml.
# Set GH_TOKEN (or GITHUB_TOKEN) in /opt/orcest/.env rather than running
# `gh auth login`, which stores an OAuth token that is subject to refresh.
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

# Create the /home/orcest ReadWritePaths targets required by the hardened
# worker systemd unit (provision/systemd/orcest-worker.service). Under
# ProtectHome=read-only systemd refuses to start a unit whose ReadWritePaths
# target is missing, so .codex (CodexRunner auth.json) and .grok (GrokRunner
# auth.json) — alongside .claude/.cache — must pre-exist. Keep this list in
# sync with that unit's ReadWritePaths line.
sudo mkdir -p /home/orcest/.claude
sudo mkdir -p /home/orcest/.cache
sudo mkdir -p /home/orcest/.codex
sudo mkdir -p /home/orcest/.grok
sudo chown -R orcest:orcest /home/orcest

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
# grok is verified softly: it's beta, and a missing binary degrades gracefully
# (grok tasks early-reject with a rebake instruction) rather than breaking the
# whole worker image.
if command -v grok &>/dev/null; then
    echo "  grok: ok"
else
    echo "  grok: MISSING (grok-backed tasks will be cleanly rejected)"
fi
ORCEST_BIN="$WORKSPACE_DIR/venv/bin/orcest"
if [[ -x "$ORCEST_BIN" ]]; then
    echo "  orcest: ok"
else
    echo "  orcest: MISSING"
    exit 1
fi

echo ""
echo "=== Setup complete ==="
