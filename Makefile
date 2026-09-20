# Makefile for ai-crew-sync — MCP coordination bus for AI coding agent teams.
#
# Everything that reaches a real environment is explicit and named as such:
# `deploy` deploys to a swarm, `up`/`down` act on the local compose stack,
# and images are published by CI on version tags — nothing here pushes one.
# `make check` is the pre-push gate and runs offline; `make test` needs
# docker for its throwaway Postgres.

# --project-directory keeps the repo root as the project dir, so compose
# reads ./.env and names the project from COMPOSE_PROJECT_NAME there even
# though the files live under Docker/.
COMPOSE      = docker compose --project-directory . -f Docker/docker-compose.yml
STACK       ?= crew
# The reverse proxy's network the bus attaches to (external in the compose
# file). `up` creates it locally when missing; `deploy` requires it to exist.
TRAEFIK_NETWORK ?= edge

TEST_PG_NAME  = ai-crew-sync-test-pg
TEST_PG_PORT ?= 55432
TEST_PG_IMAGE ?= postgres:18-alpine

.DEFAULT_GOAL := all

.PHONY: all
all: check ## Default: the pre-push gate

.PHONY: help
help: ## List every target
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# --- verification -----------------------------------------------------------

.PHONY: check
check: fmt-check lint validate config-check hooks-check ## Pre-push gate: format, clippy, compose renders, config documented, hooks sane. Offline.

.PHONY: pre-push
pre-push: check ## Alias for check

.PHONY: fmt
fmt: ## Format the Rust sources
	cargo fmt

.PHONY: fmt-check
fmt-check: ## Fail if a source file is unformatted
	cargo fmt --check

.PHONY: lint
lint: ## Clippy with warnings as errors, all targets
	cargo clippy --all-targets -- -D warnings

.PHONY: lint-fix
lint-fix: ## Apply the clippy fixes that are machine-applicable
	cargo clippy --fix --all-targets --allow-dirty -- -D warnings

# The one compose file must render from its defaults alone (a bare `make up`
# on a machine with no .env), and its build context must be the crate root:
# `config -q` validates shape but never resolves a build context, so a context
# pointing outside the repo renders fine and fails at build time.
.PHONY: validate
validate: ## Render the compose file from defaults and check its build context
	@env -u POSTGRES_PASSWORD -u BUS_VERSION -u BUS_IMAGE -u BUS_ALLOWED_HOSTS \
		-u BUS_DASHBOARD_SECRET -u BUS_PUBLIC_HOST $(COMPOSE) --env-file /dev/null config -q \
		&& echo "  ok  Docker/docker-compose.yml renders from defaults"
	@ctx=$$($(COMPOSE) config --format json 2>/dev/null \
		| python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["bus"]["build"]["context"])'); \
	if [ -d "$$ctx" ] && [ -f "$$ctx/Cargo.toml" ]; then \
		echo "  ok  + build context resolves to the crate root"; \
	else \
		echo "  FAIL  build context '$$ctx' is not the crate root"; exit 1; \
	fi

# A knob nobody can discover is a knob nobody can set. Every variable the
# server, the compose files, the client or the plugin reads must appear in
# .env.example — commented out is fine, absent is not. (Make's own variables
# are not user configuration; they are documented by `make help`.)
.PHONY: config-check
config-check: ## Fail if a configuration variable is missing from .env.example
	@missing=""; \
	for v in $$(grep -rhoE '\b(BUS|POSTGRES|DATABASE|RUST)_[A-Z_]+' \
			src Docker plugin 2>/dev/null | sort -u); do \
		case "$$v" in POSTGRES_USER|POSTGRES_DB) continue ;; esac; \
		grep -q "^#\? *$$v[=:]\?" .env.example || missing="$$missing $$v"; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "undocumented in .env.example:$$missing"; exit 1; \
	fi; \
	echo "config: every variable is documented"

.PHONY: hooks-check
hooks-check: ## Plugin hook regression tests (no bus, no network)
	@sh plugin/scripts/test-hooks.sh

.PHONY: test
test: ## Integration tests against a throwaway Postgres (needs docker)
	@docker rm -f $(TEST_PG_NAME) >/dev/null 2>&1 || true
	@docker run -d --name $(TEST_PG_NAME) -p $(TEST_PG_PORT):5432 \
		-e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
		$(TEST_PG_IMAGE) >/dev/null
	@until docker exec $(TEST_PG_NAME) pg_isready -U test >/dev/null 2>&1; do sleep 1; done
	@AI_CREW_SYNC_REQUIRE_DB=1 \
		TEST_DATABASE_URL=postgres://test:test@localhost:$(TEST_PG_PORT)/test cargo test; \
		status=$$?; docker rm -f $(TEST_PG_NAME) >/dev/null; exit $$status

# --- build and run ----------------------------------------------------------

.PHONY: build
build: ## Release build of the binary
	cargo build --release

.PHONY: run
run: ## Run the server from source against $$DATABASE_URL
	cargo run -- serve

# --- the stack --------------------------------------------------------------

# The compose file carries a build block for up-dev; `up` must never build,
# so an image that is missing locally is pulled instead.
.PHONY: up
up: network ## Start the local stack (published image), detached
	$(COMPOSE) up -d --no-build

# A local tag, so a checkout build never shadows the published image tag.
.PHONY: up-dev
up-dev: network ## Start the local stack built from this checkout
	BUS_IMAGE=ai-crew-sync BUS_VERSION=dev $(COMPOSE) up -d --build

.PHONY: network
network: ## Create the proxy network locally if it does not exist
	@docker network inspect $(TRAEFIK_NETWORK) >/dev/null 2>&1 \
		|| docker network create $(TRAEFIK_NETWORK) >/dev/null

.PHONY: down
down: ## Stop the stack (volumes are kept)
	$(COMPOSE) down

.PHONY: restart
restart: down up ## Restart the stack

.PHONY: ps
ps: ## Show stack state
	$(COMPOSE) ps

.PHONY: logs
logs: ## Follow stack logs
	$(COMPOSE) logs -f

# Preflight before the cluster, not after. The compose file has a working
# default for everything so it boots on a laptop; these checks are what keeps
# a production stack from coming up with the laptop password, a moving tag
# that makes a rolling restart non-deterministic, or per-replica dashboard
# sessions.
.PHONY: deploy-check
deploy-check: ## Verify production values are present and safe (no cluster contact)
	@fail=0; 	[ -n "$$POSTGRES_PASSWORD" ] || { echo "POSTGRES_PASSWORD is not set"; fail=1; }; 	[ "$$POSTGRES_PASSWORD" != "change-me" ] || { echo "POSTGRES_PASSWORD is still the example value"; fail=1; }; 	[ -n "$$BUS_VERSION" ] || { echo "BUS_VERSION is not set (pin an immutable tag)"; fail=1; }; 	case "$$BUS_VERSION" in latest|"") echo "BUS_VERSION must be immutable, not 'latest'"; fail=1 ;; esac; 	[ -n "$$BUS_DASHBOARD_SECRET" ] || { echo "BUS_DASHBOARD_SECRET is not set (dashboard sessions would not survive a restart or work across replicas)"; fail=1; }; 	if [ -z "$$BUS_ALLOWED_HOSTS" ]; then 		echo "BUS_ALLOWED_HOSTS is not set (use your hostname, or '*' if a proxy validates Host)"; fail=1; 	elif [ "$$BUS_ALLOWED_HOSTS" = "*" ]; then 		echo "note: BUS_ALLOWED_HOSTS=* — only safe behind a proxy that validates the Host header"; 	fi; 	if [ "$$TRAEFIK_ENABLE" = "true" ] && [ -z "$$BUS_PUBLIC_HOST" ]; then 		echo "TRAEFIK_ENABLE=true but BUS_PUBLIC_HOST is not set (the proxy needs the hostname to route)"; fail=1; 	fi; 	[ $$fail -eq 0 ] || { echo ""; echo "refusing to deploy: fix the above, then re-run"; exit 1; }; 	echo "deploy preflight: ok"

.PHONY: deploy
deploy: deploy-check ## Deploy to the current Docker Swarm as stack '$(STACK)' -- REACHES A REAL ENVIRONMENT
	@docker network inspect $(TRAEFIK_NETWORK) >/dev/null 2>&1 \
		|| { echo "network '$(TRAEFIK_NETWORK)' does not exist on this swarm: the proxy's stack must create it (attachable overlay), or set TRAEFIK_NETWORK"; exit 1; }
	docker stack deploy -c Docker/docker-compose.yml $(STACK)

# --- housekeeping -----------------------------------------------------------

.PHONY: clean
clean: ## Stop the stack and remove its volumes -- DESTROYS LOCAL DATA
	$(COMPOSE) down -v

.PHONY: dsstore
dsstore: ## Delete stray .DS_Store files
	@find . -name '.DS_Store' -type f -delete 2>/dev/null || true
