FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install gh CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update \
    && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies in a separate layer for better cache reuse
COPY pyproject.toml .
RUN pip install --no-cache-dir "redis>=5.0" "pyyaml>=6.0" "click>=8.1" "rich>=13.0" "textual>=0.75"

# Install orcest package (source only, deps already installed)
COPY src/ src/
RUN pip install --no-cache-dir --no-deps .

ENTRYPOINT ["orcest", "orchestrate"]
