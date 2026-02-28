#!/usr/bin/env bash
# Setup script for orcest orchestrator VMs.
# Run on the target VM to install Docker Engine and Docker Compose plugin.
# Tested on Ubuntu 24.04 (Noble) cloud images.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "=== Orcest Orchestrator Setup ==="

# Always run apt-get update first
echo "Updating package lists..."
sudo apt-get update -qq

# Install base packages
echo "Installing system packages..."
sudo apt-get install -y -qq \
    git \
    curl \
    ca-certificates

# Install Docker Engine (official method for Ubuntu)
if ! command -v docker &>/dev/null; then
    echo "Installing Docker Engine..."
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

    sudo apt-get update -qq
    sudo apt-get install -y -qq \
        docker-ce \
        docker-ce-cli \
        containerd.io \
        docker-buildx-plugin \
        docker-compose-plugin
fi

# Create orcest user (if not exists) and add to docker group
if ! id -u orcest &>/dev/null; then
    echo "Creating orcest user..."
    sudo useradd --system --create-home --shell /bin/bash orcest
fi
sudo usermod -aG docker orcest

# Create orchestrator directory
ORCEST_DIR="/opt/orcest"
sudo mkdir -p "$ORCEST_DIR"
sudo chown -R orcest:orcest "$ORCEST_DIR"

# Enable Docker to start on boot
sudo systemctl enable docker
sudo systemctl start docker

# Open Redis port if ufw is active (workers connect to 6379)
if command -v ufw &>/dev/null && sudo ufw status | grep -q "Status: active"; then
    echo "Opening Redis port 6379..."
    sudo ufw allow 6379/tcp
fi

# Verify
echo ""
echo "Verifying installation..."
for cmd in docker git curl; do
    if command -v "$cmd" &>/dev/null; then
        echo "  $cmd: ok"
    else
        echo "  $cmd: MISSING"
        exit 1
    fi
done

if docker compose version &>/dev/null; then
    echo "  docker compose: ok"
else
    echo "  docker compose: MISSING"
    exit 1
fi

echo ""
echo "=== Orchestrator setup complete ==="
