FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install gh CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update \
    && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/false orcest \
    && mkdir -p /home/orcest/app \
    && chown orcest:orcest /home/orcest/app

WORKDIR /home/orcest/app

# Install dependencies in a separate layer for better cache reuse
COPY --chown=orcest:orcest pyproject.toml .
RUN python3 - <<'PYEOF'
import subprocess
import sys
import tomllib
with open('pyproject.toml', 'rb') as f:
    deps = tomllib.load(f)['project']['dependencies']
subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--no-cache-dir'] + deps)
PYEOF

# Install orcest package (source only, deps already installed)
COPY --chown=orcest:orcest src/ src/
RUN pip install --no-cache-dir --no-deps .

USER orcest

ENTRYPOINT ["orcest", "orchestrate"]
