SHELL := /bin/bash
export DATABASE_URL ?= postgres://wager:wager@localhost:5432/wager?sslmode=disable
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test

.PHONY: up down deps build run test test-race vet fmt-check migrate-up migrate-down integration integration-race logs

up: ## Build and start everything (3 app instances + dependencies)
	docker compose up --build -d

down:
	docker compose down -v

deps: ## Start only PostgreSQL, Keycloak and LocalStack (for local runs and tests)
	docker compose up -d postgres keycloak localstack
	@echo "waiting for dependencies..."; \
	until [ "$$(docker compose ps --format '{{.Health}}' keycloak)" = "healthy" ] && \
	      [ "$$(docker compose ps --format '{{.Health}}' localstack)" = "healthy" ] && \
	      [ "$$(docker compose ps --format '{{.Health}}' postgres)" = "healthy" ]; do sleep 2; done; echo "ready"

build:
	go build -o bin/wager ./cmd/wager && go build -o bin/migrate ./cmd/migrate

run: build
	./bin/wager

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

integration: ## Requires `make deps`
	go test -tags integration -count=1 -timeout 20m ./test/integration/...

integration-race:
	go test -race -tags integration -count=1 -timeout 30m ./test/integration/...

logs:
	docker compose logs -f app1 app2 app3

load: ## Run the k6 load test against the 3 compose instances (needs `make up`)
	docker compose --profile load run --rm k6
