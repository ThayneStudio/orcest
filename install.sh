#!/usr/bin/env bash
# install.sh — Install orcest CLI + OpenTofu on the Proxmox host.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/ThayneStudio/orcest/master/install.sh | bash
#   # or locally:
#   bash install.sh
set -euo pipefail

VENV_DIR="/opt/orcest-cli"
CONFIG_DIR="/etc/orcest"
TOFU_VERSION="1.9.0"
REPO_URL="https://github.com/ThayneStudio/orcest.git"

info()  { printf '\033[1;34m==> %s\033[0m\n' "$*"; }
warn()  { printf '\033[1;33m==> %s\033[0m\n' "$*"; }
die()   { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ── Pre-flight checks ──────────────────────────────────────
[[ $EUID -eq 0 ]] || die "Must run as root"

if ! command -v pvesh &>/dev/null; then
    warn "pvesh not found — this does not appear to be a Proxmox host."
    warn "Continuing anyway (you can still use orcest fleet commands)."
fi

# ── System dependencies ────────────────────────────────────
info "Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git curl

# ── Install OpenTofu ───────────────────────────────────────
if command -v tofu &>/dev/null; then
    info "OpenTofu already installed: $(tofu version | head -1)"
else
    info "Installing OpenTofu ${TOFU_VERSION}..."
    ARCH=$(dpkg --print-architecture)
    TOFU_URL="https://github.com/opentofu/opentofu/releases/download/v${TOFU_VERSION}/tofu_${TOFU_VERSION}_${ARCH}.deb"
    TMP_DEB=$(mktemp /tmp/tofu-XXXXXX.deb)
    curl -fsSL -o "$TMP_DEB" "$TOFU_URL"
    dpkg -i "$TMP_DEB"
    rm -f "$TMP_DEB"
    info "OpenTofu installed: $(tofu version | head -1)"
fi

# ── Create venv and install orcest ─────────────────────────
info "Creating Python venv at ${VENV_DIR}..."
python3 -m venv "$VENV_DIR"

info "Installing orcest..."
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
"${VENV_DIR}/bin/pip" install --quiet "git+${REPO_URL}"

# ── Symlink CLI ────────────────────────────────────────────
ln -sf "${VENV_DIR}/bin/orcest" /usr/local/bin/orcest

# ── Config directory ───────────────────────────────────────
mkdir -p "${CONFIG_DIR}/terraform"

info "Installation complete."
echo ""
echo "  orcest CLI: /usr/local/bin/orcest"
echo "  Config dir: ${CONFIG_DIR}/"
echo ""
echo "  Next step: orcest init"
