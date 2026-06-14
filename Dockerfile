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

RUN useradd --create-home --shell /bin/false orcest \
    && mkdir -p /home/orcest/app \
    && chown orcest:orcest /home/orcest/app

WORKDIR /home/orcest/app

# Install pinned dependencies from the lock for reproducible builds.
# (Regenerate with `make lock`; never resolve unpinned ranges at build time.)
COPY --chown=orcest:orcest requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

# Install the orcest package itself. --no-deps means pyproject's unpinned
# ranges are NOT re-resolved — every dependency already came from the lock
# above; pyproject.toml is needed only to build the package metadata.
COPY --chown=orcest:orcest pyproject.toml .
COPY --chown=orcest:orcest src/ src/
RUN pip install --no-cache-dir --no-deps .

USER orcest

ENTRYPOINT ["orcest", "orchestrate"]
