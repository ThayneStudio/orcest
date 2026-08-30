.PHONY: test test-unit check-dashboard-tracked check-dashboard-release-revision check-dashboard-clean-copy test-dashboard audit-dashboard redis-up redis-down lint format lock lock-dev check-lock-dev build-dashboard smoke-dashboard-image smoke-dashboard-compose dev-dashboard check-dashboard-remote-paths preflight-dashboard-remote sync-dashboard-remote sync-dashboard-remote-unlocked deploy-dashboard deploy-dashboard-remote

PIP_COMPILE_CMD ?= pip-compile
DASHBOARD_REDIS_ENV ?= /opt/orcest/.redis.env
DASHBOARD_ENV ?= /opt/orcest/.dashboard.env
DASHBOARD_NODE_VERSION ?= $(shell cat dashboard/.node-version)
DASHBOARD_NODE_IMAGE ?= node:$(DASHBOARD_NODE_VERSION)-slim
DASHBOARD_AUDIT_LEVEL ?= moderate
ORCEST_BUILD_REVISION ?= $(shell git rev-parse HEAD 2>/dev/null)
DASHBOARD_NPM_ENV = -e NPM_CONFIG_AUDIT=false -e NPM_CONFIG_FUND=false -e NPM_CONFIG_PROGRESS=false -e NPM_CONFIG_UPDATE_NOTIFIER=false
DASHBOARD_DOCKER_RUN = docker run --rm --user "$(shell id -u):$(shell id -g)" -e HOME=/tmp $(DASHBOARD_NPM_ENV) -v "$(CURDIR)/dashboard:/app" -w /app $(DASHBOARD_NODE_IMAGE)
DASHBOARD_SOURCE_TAR_EXCLUDES = --exclude='./node_modules' --exclude='./dist' --exclude='./build' --exclude='./.git' --exclude='./.env' --exclude='./.env.*' --exclude='./*.env' --exclude='./.npmrc*' --exclude='./npm-debug.log*' --exclude='./vite.config.ts.timestamp-*.mjs'
DASHBOARD_RSYNC_EXCLUDES = --exclude='node_modules/' --exclude='dist/' --exclude='build/' --exclude='.git/' --exclude='.env' --exclude='.env.*' --exclude='*.env' --exclude='.npmrc*' --exclude='npm-debug.log*' --exclude='vite.config.ts.timestamp-*.mjs'
DASHBOARD_READY_NODE = fetch("http://127.0.0.1:8080/api/ready").then(async (res) => { console.log(await res.text()); if (!res.ok) process.exit(1); }).catch((err) => { console.error(err.message); process.exit(1); })
DASHBOARD_READY_NODE_ESCAPED = fetch(\"http://127.0.0.1:8080/api/ready\").then(async (res) => { console.log(await res.text()); if (!res.ok) process.exit(1); }).catch((err) => { console.error(err.message); process.exit(1); })
DASHBOARD_REMOTE ?=
DASHBOARD_REMOTE_DIR ?= /opt/orcest/dashboard
DASHBOARD_REMOTE_ORCEST_DIR ?= /opt/orcest
DASHBOARD_REMOTE_DEPLOY_LOCK_DIR ?= $(DASHBOARD_REMOTE_ORCEST_DIR)/.dashboard-deploy.lock
DASHBOARD_COMPOSE_STATE_FILE ?= $(CURDIR)/.dashboard-compose.last-known-good.yml
DASHBOARD_REMOTE_COMPOSE_STATE_FILE ?= $(DASHBOARD_REMOTE_ORCEST_DIR)/.dashboard-compose.last-known-good.yml
DASHBOARD_REMOTE_RSYNC_SHELL ?= ssh -o BatchMode=yes
DASHBOARD_REMOTE_EXEC ?= ssh -o BatchMode=yes $(DASHBOARD_REMOTE)
DASHBOARD_REMOTE_COMPOSE ?= docker compose --env-file .redis.env --env-file .dashboard.env -f docker-compose.dashboard.yml
DASHBOARD_REMOTE_PUBLISHED_ENV = ORCEST_BUILD_REVISION=$(call DASHBOARD_SHELL_QUOTE,$(ORCEST_BUILD_REVISION)) DASHBOARD_EXPECTED_REVISION=$(call DASHBOARD_SHELL_QUOTE,$(ORCEST_BUILD_REVISION)) $(if $(DASHBOARD_READY_ATTEMPTS),DASHBOARD_READY_ATTEMPTS=$(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_READY_ATTEMPTS)) ,)$(if $(DASHBOARD_READY_INTERVAL_MS),DASHBOARD_READY_INTERVAL_MS=$(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_READY_INTERVAL_MS)) ,)$(if $(DASHBOARD_ALLOW_DEGRADED),DASHBOARD_ALLOW_DEGRADED=$(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_ALLOW_DEGRADED)) ,)$(if $(DASHBOARD_STRICT_DEGRADED),DASHBOARD_STRICT_DEGRADED=$(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_STRICT_DEGRADED)) ,)$(if $(DASHBOARD_ALLOW_UNPINNED_ASSETS),DASHBOARD_ALLOW_UNPINNED_ASSETS=$(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_ALLOW_UNPINNED_ASSETS)) ,)
DASHBOARD_DEV_API_HOST_PORT ?= 8080
DASHBOARD_DEV_VITE_HOST_PORT ?= 5173
DASHBOARD_DEV_REDIS_HOST ?= host.docker.internal
DASHBOARD_DEV_REDIS_PORT ?= 6379
DASHBOARD_DEV_DOCKER_FLAGS ?= -it
DASHBOARD_DEV_DOCKER_ARGS ?= --add-host=host.docker.internal:host-gateway
DASHBOARD_DEV_INSTALL ?= npm ci
DASHBOARD_SHELL_QUOTE = '$(subst ','\'',$(1))'
DASHBOARD_REMOTE_DIR_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_DIR))
DASHBOARD_REMOTE_ORCEST_DIR_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_ORCEST_DIR))
DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR))
DASHBOARD_REMOTE_COMPOSE_STATE_FILE_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_COMPOSE_STATE_FILE))
DASHBOARD_NODE_VERSION_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_NODE_VERSION))
DASHBOARD_NODE_IMAGE_SH = $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_NODE_IMAGE))

define DASHBOARD_RUN_IN_CLEAN_COPY
tar -C "$(CURDIR)/dashboard" $(DASHBOARD_SOURCE_TAR_EXCLUDES) -cf - . | \
docker run --rm -i -e HOME=/tmp $(DASHBOARD_NPM_ENV) -w /app $(DASHBOARD_NODE_IMAGE) sh -lc 'tar -C /app -xf - && $(1)'
endef

define DASHBOARD_RUN_REMOTE
printf '%s\n' $(call DASHBOARD_SHELL_QUOTE,$(1)) | $(DASHBOARD_REMOTE_EXEC) sh -eu
endef

# Unit tests only (no Redis needed)
test-unit:
	pytest -m unit --cov=src/orcest --cov-report=term-missing

check-dashboard-tracked:
	sh dashboard/scripts/check-tracked-files.sh

check-dashboard-release-revision:
	@revision=$(call DASHBOARD_SHELL_QUOTE,$(ORCEST_BUILD_REVISION)); \
	actual=$$(git rev-parse HEAD 2>/dev/null || true); \
	printf '%s\n' "$$revision" | grep -Eq '^[0-9a-f]{7,64}$$' || { echo "ORCEST_BUILD_REVISION must be an exact clean Git revision"; exit 1; }; \
	[ "$$revision" = "$$actual" ] || { echo "ORCEST_BUILD_REVISION does not match the checked-out commit $$actual"; exit 1; }; \
	[ -z "$$(git status --porcelain 2>/dev/null)" ] || { echo "Dashboard deployment requires a clean Git checkout"; exit 1; }

check-dashboard-clean-copy: check-dashboard-tracked
	$(call DASHBOARD_RUN_IN_CLEAN_COPY,test -f package.json && test -f scripts/check-node-version.mjs && test ! -e node_modules && test ! -e dist && test ! -e build && touch .clean-copy-write-check && rm .clean-copy-write-check && npm run check:node)

test-dashboard: check-dashboard-tracked
	$(call DASHBOARD_RUN_IN_CLEAN_COPY,npm ci && npm run typecheck && npm test && npm run build && npm run check:bundle-runtime)

# Dependency audit, deliberately OUTSIDE the correctness chain above. `npm audit`
# queries the live registry, so a brand-new advisory against a transitive dev
# dependency — or a registry outage — would otherwise turn every PR red without
# a code change, and would do so BEFORE typecheck/tests/build ever ran. CI runs
# this as its own non-blocking step so the signal stays visible.
audit-dashboard: check-dashboard-tracked
	$(call DASHBOARD_RUN_IN_CLEAN_COPY,npm ci && npm audit --audit-level=$(DASHBOARD_AUDIT_LEVEL))

# All tests — starts Redis, runs everything, stops Redis
test: redis-up
	pytest -v --cov=src/orcest --cov-report=term-missing; ret=$$?; \
	if [ $$ret -eq 0 ]; then $(MAKE) test-dashboard; ret=$$?; fi; \
	$(MAKE) redis-down; exit $$ret

# Start Redis in Docker for integration/stress tests
redis-up:
	docker compose -f docker-compose.redis.yml up -d redis
	@echo "Waiting for Redis..."
	@timeout 30 bash -c 'until docker compose -f docker-compose.redis.yml exec redis redis-cli ping 2>/dev/null | grep -q PONG; do sleep 0.2; done'

redis-down:
	docker compose -f docker-compose.redis.yml stop redis
	docker compose -f docker-compose.redis.yml rm -f redis

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

lock:
	pip-compile pyproject.toml --output-file requirements.lock --strip-extras

lock-dev:
	$(PIP_COMPILE_CMD) pyproject.toml --extra dev --all-build-deps --constraint requirements.lock --constraint requirements-dev-toolchain.txt --output-file requirements-dev.lock --strip-extras --allow-unsafe --no-header

check-lock-dev:
	@tmp=$$(mktemp); \
	trap 'rm -f "$$tmp"' EXIT INT TERM; \
	$(PIP_COMPILE_CMD) --quiet pyproject.toml --extra dev --all-build-deps --constraint requirements.lock --constraint requirements-dev-toolchain.txt --output-file "$$tmp" --strip-extras --allow-unsafe --no-header; \
	diff -u requirements-dev.lock "$$tmp"

build-dashboard: check-dashboard-tracked
	$(call DASHBOARD_RUN_IN_CLEAN_COPY,npm ci && npm run build)

smoke-dashboard-image: check-dashboard-tracked
	docker build --build-arg NODE_VERSION="$(DASHBOARD_NODE_VERSION)" -t orcest-dashboard:local-check dashboard
	sh dashboard/scripts/smoke-image.sh orcest-dashboard:local-check

smoke-dashboard-compose: check-dashboard-tracked
	DASHBOARD_NODE_VERSION="$(DASHBOARD_NODE_VERSION)" sh dashboard/scripts/smoke-compose.sh

dev-dashboard:
	@token="$${DASHBOARD_TOKEN:-dev-dashboard-token}"; \
	redis_password="$${REDIS_PASSWORD:-$${ORCEST_REDIS_PASSWORD:-}}"; \
	echo "Dashboard dev UI: http://127.0.0.1:$(DASHBOARD_DEV_VITE_HOST_PORT)/?token=$$token"; \
	DASHBOARD_TOKEN="$$token" \
	REDIS_PASSWORD="$$redis_password" \
	ORCEST_REDIS_PASSWORD="$$redis_password" \
	docker run --rm $(DASHBOARD_DEV_DOCKER_FLAGS) \
		$(DASHBOARD_DEV_DOCKER_ARGS) \
		--user "$(shell id -u):$(shell id -g)" \
		-e HOME=/tmp \
		$(DASHBOARD_NPM_ENV) \
		-e REDIS_HOST="$(DASHBOARD_DEV_REDIS_HOST)" \
		-e REDIS_PORT="$(DASHBOARD_DEV_REDIS_PORT)" \
		-e REDIS_PASSWORD \
		-e ORCEST_REDIS_PASSWORD \
		-e DASHBOARD_TOKEN \
		-e DASHBOARD_REDIS_PREFIXES \
		-p 127.0.0.1:$(DASHBOARD_DEV_API_HOST_PORT):8080 \
		-p 127.0.0.1:$(DASHBOARD_DEV_VITE_HOST_PORT):5173 \
		-v "$(CURDIR)/dashboard:/app" \
		-w /app \
		$(DASHBOARD_NODE_IMAGE) sh -lc '$(DASHBOARD_DEV_INSTALL) && npm run dev:docker'

check-dashboard-remote-paths: check-dashboard-tracked
	@test -n $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE)) || { echo "Set DASHBOARD_REMOTE=user@host"; exit 1; }
	@remote_dir=$$(printf '%s' $(DASHBOARD_REMOTE_DIR_SH) | sed 's:/*$$::'); \
	orcest_dir=$$(printf '%s' $(DASHBOARD_REMOTE_ORCEST_DIR_SH) | sed 's:/*$$::'); \
	state_file=$$(printf '%s' $(DASHBOARD_REMOTE_COMPOSE_STATE_FILE_SH) | sed 's:/*$$::'); \
	expected_dir="$$orcest_dir/dashboard"; \
	case "$$orcest_dir" in ""|"/"|"/opt"|"/var"|"/usr"|"/home"|"/root"|"/etc"|"/bin"|"/sbin"|"/lib"|"/lib64"|"/dev"|"/proc"|"/sys"|"/run"|"/tmp"|"/boot"|"/mnt"|"/media"|".."|*"/.."|*"/../"*|"../"*|"."|"./"*) \
		echo "Refusing unsafe DASHBOARD_REMOTE_ORCEST_DIR=$$orcest_dir"; \
		exit 1; \
		;; \
	esac; \
	case "$$orcest_dir" in /opt/*) ;; \
		*) \
			echo "Refusing unsafe DASHBOARD_REMOTE_ORCEST_DIR=$$orcest_dir; expected an /opt/... compose root"; \
			exit 1; \
			;; \
	esac; \
	case "$$remote_dir" in ""|"/"|"/opt"|"$$orcest_dir") \
		echo "Refusing unsafe DASHBOARD_REMOTE_DIR=$$remote_dir; expected $$expected_dir"; \
		exit 1; \
		;; \
	esac; \
	case "$$remote_dir" in ".."|*"/.."|*"/../"*|"../"*|"."|"./"*) \
		echo "Refusing unsafe DASHBOARD_REMOTE_DIR=$$remote_dir; expected $$expected_dir"; \
		exit 1; \
		;; \
	esac; \
	if [ "$$remote_dir" != "$$expected_dir" ]; then \
		echo "DASHBOARD_REMOTE_DIR must be $$expected_dir because docker-compose.dashboard.yml builds ./dashboard"; \
		exit 1; \
	fi; \
	case "$$state_file" in /*) ;; \
		*) \
			echo "DASHBOARD_REMOTE_COMPOSE_STATE_FILE must be an absolute path outside DASHBOARD_REMOTE_DIR"; \
			exit 1; \
			;; \
	esac; \
	case "$$state_file" in ""|"."|".."|*"/.."|*"/../"*|"../"*) \
		echo "Refusing unsafe DASHBOARD_REMOTE_COMPOSE_STATE_FILE=$$state_file"; \
		exit 1; \
		;; \
	esac; \
	case "$$state_file" in "$$remote_dir"|"$$remote_dir"/*) \
		echo "DASHBOARD_REMOTE_COMPOSE_STATE_FILE must be outside DASHBOARD_REMOTE_DIR because dashboard sync uses rsync --delete"; \
		exit 1; \
		;; \
	esac; \
	if [ "$$state_file" = "$$orcest_dir" ]; then \
		echo "Refusing unsafe DASHBOARD_REMOTE_COMPOSE_STATE_FILE=$$state_file"; \
		exit 1; \
	fi

preflight-dashboard-remote: check-dashboard-remote-paths
	$(call DASHBOARD_RUN_REMOTE,cd $(DASHBOARD_REMOTE_ORCEST_DIR_SH) && test -r .redis.env || { echo "Missing $(DASHBOARD_REMOTE_ORCEST_DIR)/.redis.env"; exit 1; }; test -r .dashboard.env || { echo "Missing $(DASHBOARD_REMOTE_ORCEST_DIR)/.dashboard.env"; exit 1; }; grep -Eq '^ORCEST_REDIS_PASSWORD=.+$$' .redis.env || { echo "Missing ORCEST_REDIS_PASSWORD in $(DASHBOARD_REMOTE_ORCEST_DIR)/.redis.env"; exit 1; }; grep -Eq '^DASHBOARD_TOKEN=.+$$' .dashboard.env || { echo "Missing DASHBOARD_TOKEN in $(DASHBOARD_REMOTE_ORCEST_DIR)/.dashboard.env"; exit 1; })

sync-dashboard-remote: check-dashboard-remote-paths
	@set -eu; \
	remote_lock_acquired=0; \
	release_remote_lock() { \
		$(call DASHBOARD_RUN_REMOTE,lock_dir=$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH); unsafe=0; [ -n "$$lock_dir" ] || unsafe=1; [ "$$lock_dir" != "/" ] || unsafe=1; [ "$$lock_dir" != "." ] || unsafe=1; [ "$$lock_dir" != ".." ] || unsafe=1; if expr "$$lock_dir" : "\\.\\./" >/dev/null; then unsafe=1; fi; if expr "$$lock_dir" : ".*/\\.\\." >/dev/null; then unsafe=1; fi; if [ "$$unsafe" = "1" ]; then echo "Unsafe DASHBOARD_REMOTE_DEPLOY_LOCK_DIR: $$lock_dir" >&2; exit 2; fi; rm -rf "$$lock_dir") || true; \
	}; \
	trap 'status=$$?; if [ "$$remote_lock_acquired" = "1" ]; then release_remote_lock; fi; exit $$status' EXIT INT TERM; \
	$(call DASHBOARD_RUN_REMOTE,lock_dir=$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH); unsafe=0; [ -n "$$lock_dir" ] || unsafe=1; [ "$$lock_dir" != "/" ] || unsafe=1; [ "$$lock_dir" != "." ] || unsafe=1; [ "$$lock_dir" != ".." ] || unsafe=1; if expr "$$lock_dir" : "\\.\\./" >/dev/null; then unsafe=1; fi; if expr "$$lock_dir" : ".*/\\.\\." >/dev/null; then unsafe=1; fi; if [ "$$unsafe" = "1" ]; then echo "Unsafe DASHBOARD_REMOTE_DEPLOY_LOCK_DIR: $$lock_dir" >&2; exit 2; fi; if mkdir "$$lock_dir" 2>/dev/null; then { printf "%s\n" "pid=$$$$"; printf "%s\n" "started_at_epoch=$$(date +%s 2>/dev/null || true)"; printf "%s\n" "target=sync-dashboard-remote"; } >"$$lock_dir/info" || true; elif [ ! -d "$$lock_dir" ]; then echo "Dashboard deploy lock could not be created: $$lock_dir" >&2; exit 1; else echo "Dashboard deploy lock is already held: $$lock_dir" >&2; if [ -r "$$lock_dir/info" ]; then sed 's/^/  /' "$$lock_dir/info" >&2 || true; fi; echo "Refusing to run a concurrent dashboard sync/deploy; remove the lock only after confirming no deploy is active." >&2; exit 1; fi); \
	remote_lock_acquired=1; \
	make sync-dashboard-remote-unlocked

sync-dashboard-remote-unlocked: preflight-dashboard-remote
	$(call DASHBOARD_RUN_REMOTE,docker compose version >/dev/null; network_name=$$(awk 'function trim(s) { sub(/^[[:space:]]+/, "", s); sub(/[[:space:]]+$$/, "", s); return s } function unquote(s, q, body, end) { s=trim(s); q=substr(s, 1, 1); if (q == "\"" || q == sprintf("%c", 39)) { body=substr(s, 2); end=index(body, q); return end > 0 ? substr(body, 1, end - 1) : body } sub(/[[:space:]]+#.*$$/, "", s); return trim(s) } /^[[:space:]]*(#|$$)/ { next } /^[[:space:]]*ORCEST_DOCKER_NETWORK[[:space:]]*=/ { value=$$0; sub(/^[[:space:]]*ORCEST_DOCKER_NETWORK[[:space:]]*=[[:space:]]*/, "", value); found=1; result=unquote(value) } END { if (found) print result }' $(DASHBOARD_REMOTE_ORCEST_DIR_SH)/.redis.env $(DASHBOARD_REMOTE_ORCEST_DIR_SH)/.dashboard.env 2>/dev/null); network_name="$${ORCEST_DOCKER_NETWORK:-$${network_name:-orcest}}"; docker network inspect "$$network_name" >/dev/null || { echo "Missing external Docker network '$$network_name'; deploy Redis/network before dashboard"; exit 1; })
	$(call DASHBOARD_RUN_REMOTE,orcest_dir=$$(printf '%s' $(DASHBOARD_REMOTE_ORCEST_DIR_SH) | sed 's:/*$$::'); remote_dir=$$(printf '%s' $(DASHBOARD_REMOTE_DIR_SH) | sed 's:/*$$::'); mkdir -p "$$orcest_dir" "$$remote_dir"; test -d "$$orcest_dir" && test -w "$$orcest_dir" && test -d "$$remote_dir" && test -w "$$remote_dir" || { echo "Remote dashboard directories are not writable: $$orcest_dir $$remote_dir"; exit 1; }; test ! -L "$$orcest_dir" && test ! -L "$$remote_dir" || { echo "Remote dashboard directories must not be symlinks: $$orcest_dir $$remote_dir"; exit 1; }; orcest_physical=$$(cd "$$orcest_dir" && pwd -P); remote_physical=$$(cd "$$remote_dir" && pwd -P); { [ "$$orcest_physical" = "$$orcest_dir" ] && [ "$$remote_physical" = "$$remote_dir" ]; } || { echo "Remote dashboard directories must resolve to configured paths: $$orcest_dir $$remote_dir"; exit 1; })
	$(call DASHBOARD_RUN_REMOTE,compose_file=$(DASHBOARD_REMOTE_ORCEST_DIR_SH)/docker-compose.dashboard.yml; state_file=$(DASHBOARD_REMOTE_COMPOSE_STATE_FILE_SH); if [ -e "$$state_file" ] && { [ ! -f "$$state_file" ] || [ -L "$$state_file" ]; }; then echo "Dashboard Compose state file must be a regular file (not a symlink): $$state_file" >&2; exit 1; fi; if [ ! -e "$$state_file" ] && [ -r "$$compose_file" ]; then state_tmp=$$(mktemp "$${state_file}.tmp.XXXXXX"); cp -p "$$compose_file" "$$state_tmp" && mv "$$state_tmp" "$$state_file" || { rm -f "$$state_tmp"; echo "Could not seed dashboard last-known-good Compose state" >&2; exit 1; }; fi)
	rsync -az --delete $(DASHBOARD_RSYNC_EXCLUDES) -e $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_RSYNC_SHELL)) $(call DASHBOARD_SHELL_QUOTE,$(CURDIR)/dashboard/) $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE):$(DASHBOARD_REMOTE_DIR)/)
	rsync -az -e $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE_RSYNC_SHELL)) $(call DASHBOARD_SHELL_QUOTE,$(CURDIR)/docker-compose.dashboard.yml) $(call DASHBOARD_SHELL_QUOTE,$(DASHBOARD_REMOTE):$(DASHBOARD_REMOTE_ORCEST_DIR)/docker-compose.dashboard.yml)

deploy-dashboard: check-dashboard-tracked check-dashboard-release-revision
	@test -r "$(DASHBOARD_REDIS_ENV)" || { echo "Missing $(DASHBOARD_REDIS_ENV); deploy Redis first or set DASHBOARD_REDIS_ENV"; exit 1; }
	@if [ -z "$${ORCEST_REDIS_PASSWORD:-}" ] && ! grep -Eq '^ORCEST_REDIS_PASSWORD=.+$$' "$(DASHBOARD_REDIS_ENV)" 2>/dev/null; then \
		echo "Missing ORCEST_REDIS_PASSWORD; export it or define it in $(DASHBOARD_REDIS_ENV)"; \
		exit 1; \
	fi
	@if [ -z "$${DASHBOARD_TOKEN:-}" ] && ! grep -Eq '^DASHBOARD_TOKEN=.+$$' "$(DASHBOARD_ENV)" 2>/dev/null; then \
		echo "Missing DASHBOARD_TOKEN; export it or create $(DASHBOARD_ENV) with DASHBOARD_TOKEN=..."; \
		exit 1; \
	fi
	@set -- --env-file "$(DASHBOARD_REDIS_ENV)"; \
	if [ -r "$(DASHBOARD_ENV)" ]; then set -- "$$@" --env-file "$(DASHBOARD_ENV)"; fi; \
	published_env_file=""; \
	if [ -r "$(DASHBOARD_ENV)" ]; then published_env_file="$(DASHBOARD_ENV)"; fi; \
	set -- docker compose "$$@" -f docker-compose.dashboard.yml; \
	ORCEST_BUILD_REVISION="$(ORCEST_BUILD_REVISION)" DASHBOARD_EXPECTED_REVISION="$(ORCEST_BUILD_REVISION)" DASHBOARD_NODE_VERSION="$(DASHBOARD_NODE_VERSION)" DASHBOARD_NODE_IMAGE="$(DASHBOARD_NODE_IMAGE)" DASHBOARD_BASE_URL="http://127.0.0.1:8080" DASHBOARD_ENV_FILE="$$published_env_file" DASHBOARD_COMPOSE_FILE="$(CURDIR)/docker-compose.dashboard.yml" DASHBOARD_COMPOSE_STATE_FILE="$(DASHBOARD_COMPOSE_STATE_FILE)" sh dashboard/scripts/deploy-compose-dashboard.sh "$$@"

deploy-dashboard-remote: check-dashboard-remote-paths check-dashboard-release-revision
	@set -eu; \
	remote_lock_acquired=0; \
	release_remote_lock() { \
		$(call DASHBOARD_RUN_REMOTE,lock_dir=$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH); unsafe=0; [ -n "$$lock_dir" ] || unsafe=1; [ "$$lock_dir" != "/" ] || unsafe=1; [ "$$lock_dir" != "." ] || unsafe=1; [ "$$lock_dir" != ".." ] || unsafe=1; if expr "$$lock_dir" : "\\.\\./" >/dev/null; then unsafe=1; fi; if expr "$$lock_dir" : ".*/\\.\\." >/dev/null; then unsafe=1; fi; if [ "$$unsafe" = "1" ]; then echo "Unsafe DASHBOARD_REMOTE_DEPLOY_LOCK_DIR: $$lock_dir" >&2; exit 2; fi; rm -rf "$$lock_dir") || true; \
	}; \
	trap 'status=$$?; if [ "$$remote_lock_acquired" = "1" ]; then release_remote_lock; fi; exit $$status' EXIT INT TERM; \
	$(call DASHBOARD_RUN_REMOTE,lock_dir=$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH); unsafe=0; [ -n "$$lock_dir" ] || unsafe=1; [ "$$lock_dir" != "/" ] || unsafe=1; [ "$$lock_dir" != "." ] || unsafe=1; [ "$$lock_dir" != ".." ] || unsafe=1; if expr "$$lock_dir" : "\\.\\./" >/dev/null; then unsafe=1; fi; if expr "$$lock_dir" : ".*/\\.\\." >/dev/null; then unsafe=1; fi; if [ "$$unsafe" = "1" ]; then echo "Unsafe DASHBOARD_REMOTE_DEPLOY_LOCK_DIR: $$lock_dir" >&2; exit 2; fi; if mkdir "$$lock_dir" 2>/dev/null; then { printf "%s\n" "pid=$$$$"; printf "%s\n" "started_at_epoch=$$(date +%s 2>/dev/null || true)"; printf "%s\n" "target=deploy-dashboard-remote"; } >"$$lock_dir/info" || true; elif [ ! -d "$$lock_dir" ]; then echo "Dashboard deploy lock could not be created: $$lock_dir" >&2; exit 1; else echo "Dashboard deploy lock is already held: $$lock_dir" >&2; if [ -r "$$lock_dir/info" ]; then sed 's/^/  /' "$$lock_dir/info" >&2 || true; fi; echo "Refusing to run a concurrent dashboard sync/deploy; remove the lock only after confirming no deploy is active." >&2; exit 1; fi); \
	remote_lock_acquired=1; \
	make sync-dashboard-remote-unlocked; \
	$(call DASHBOARD_RUN_REMOTE,cd $(DASHBOARD_REMOTE_ORCEST_DIR_SH) && $(DASHBOARD_REMOTE_PUBLISHED_ENV)DASHBOARD_DEPLOY_LOCK_HELD=1 DASHBOARD_DEPLOY_LOCK_DIR=$(DASHBOARD_REMOTE_DEPLOY_LOCK_DIR_SH) DASHBOARD_NODE_VERSION=$(DASHBOARD_NODE_VERSION_SH) DASHBOARD_NODE_IMAGE=$(DASHBOARD_NODE_IMAGE_SH) DASHBOARD_BASE_URL='http://127.0.0.1:8080' DASHBOARD_ENV_FILE='.dashboard.env' DASHBOARD_COMPOSE_FILE='docker-compose.dashboard.yml' DASHBOARD_COMPOSE_STATE_FILE=$(DASHBOARD_REMOTE_COMPOSE_STATE_FILE_SH) sh dashboard/scripts/deploy-compose-dashboard.sh $(DASHBOARD_REMOTE_COMPOSE))
