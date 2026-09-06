.PHONY: test test-all run migrate sqlc lint tidy build image compose-up compose-down

# The copied config wins when it exists (README quick start); the example otherwise.
CONFIG ?= $(if $(wildcard config/config.yaml),config/config.yaml,config/config.example.yaml)

test:
	go test -p=1 ./...

test-all:
	./scripts/test-all.sh

build:
	go build -o bin/nexo ./cmd/nexo

# Fresh per invocation: config.Validate rejects short and published secrets, and a literal here
# would be exactly the kind of value that ends up deployed.
run: export NEXO_AUTH_NATIVE_SECRET ?= $(shell head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
run:
	go run ./cmd/nexo serve -config $(CONFIG)

migrate:
	go run ./cmd/nexo migrate -config $(CONFIG)

# Run out-of-module: a `tool` directive would drag sqlc's compiler (antlr, cel-go, grpc, protobuf,
# a wasm sqlite) into the graph of every project that imports server/, sdk/ or msgbody/.
SQLC_VERSION ?= v1.31.2-0.20260820204440-dfb6bda4389b

sqlc:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate -f sqlc.yaml

lint:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not found, skipping (go install honnef.co/go/tools/cmd/staticcheck@v0.8.1); CI runs it"; \
	fi

tidy:
	go mod tidy

GOPROXY_ARG ?= $(shell go env GOPROXY)

image:
	docker build --build-arg GOPROXY=$(GOPROXY_ARG) -t nexo:dev .

# config.pg-only.yaml runs on PG alone; every other config gets the Redis overlay.
COMPOSE_FILES = -f deploy/docker-compose.yml $(if $(filter config.pg-only.yaml,$(NEXO_COMPOSE_CONFIG)),,-f deploy/docker-compose.redis.yml)

compose-up: image
	docker compose $(COMPOSE_FILES) up -d

compose-down:
	docker compose $(COMPOSE_FILES) down -v
