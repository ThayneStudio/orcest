FROM python:3.12-slim

ARG ORCEST_BUILD_REVISION=unknown
ARG ORCEST_UID=1000
ARG ORCEST_GID=1000
ENV ORCEST_BUILD_REVISION=${ORCEST_BUILD_REVISION}
LABEL org.opencontainers.image.revision=${ORCEST_BUILD_REVISION}

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

RUN groupadd --gid "${ORCEST_GID}" orcest \
    && useradd --uid "${ORCEST_UID}" --gid "${ORCEST_GID}" \
        --create-home --shell /bin/false orcest \
    && mkdir -p /home/orcest/app \
    && chown orcest:orcest /home/orcest/app

WORKDIR /home/orcest/app

# Install pinned dependencies from the lock for reproducible builds.
# Mirror of the repo-root requirements.lock, shipped as package data so it is
# present in the deploy build context (assembled by
# orcest.fleet.orchestrator.upload_source). Regenerate with `make lock` and
# copy into this dir alongside pyproject.toml. NB: upload_source must include
# "requirements.lock" in its deploy_files tuple for this COPY to resolve.
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
