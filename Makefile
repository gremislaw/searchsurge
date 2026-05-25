.PHONY: up down build restart test test-race proto-gen proto-setup bench logs lint clean help

up:
	docker compose up -d

down:
	docker compose down --remove-orphans

restart: down up

build:
	docker compose build --no-cache

logs:
	docker compose logs -f searchsurge-master searchsurge-slave nginx

test:
	go test ./... -v -count=1

test-race:
	go test ./... -v -race -count=1

test-integration:
	@echo "Starting integration environment..."
	make up
	@echo "Waiting for services..."
	@for i in 1 2 3 4 5; do \
		curl -s http://localhost:8081/health > /dev/null && break || sleep 2; \
	done
	@echo "Running integration tests..."
	INTEGRATION=1 go test -v -tags=integration -timeout=5m ./tests/integration/...
	@echo "Cleaning up..."
	docker compose down

lint:
	golangci-lint run ./...

proto-setup:
	@if [ ! -d "googleapis" ]; then \
		echo "Cloning googleapis for proto imports..."; \
		git clone https://github.com/googleapis/googleapis.git --depth 1; \
	fi

proto-gen: proto-setup
	mkdir -p internal/pb
	protoc --proto_path=. \
	  --proto_path=./googleapis \
	  --go_out=internal/pb --go_opt=paths=source_relative \
	  --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
	  --grpc-gateway_out=internal/pb --grpc-gateway_opt=paths=source_relative \
	  proto/api.proto

bench:
	@if ! command -v k6 >/dev/null 2>&1; then \
		echo "k6 not found. Install: https://k6.io/docs/getting-started/installation/"; \
		exit 1; \
	fi
	k6 run scripts/loadtest.js

clean:
	rm -rf googleapis
	docker compose down -v --remove-orphans
	docker system prune -f

help:
	@echo "Available targets:"
	@echo "  make up               - Start all services (detached)"
	@echo "  make down             - Stop and remove containers"
	@echo "  make restart          - Restart all services"
	@echo "  make build            - Rebuild Docker images"
	@echo "  make logs             - Follow logs of searchsurge nodes"
	@echo "  make test             - Run unit tests"
	@echo "  make test-race        - Run tests with -race detector"
	@echo "  make lint             - Run golangci-lint"
	@echo "  make proto-gen        - Generate protobuf Go code"
	@echo "  make bench            - Run k6 load test"
	@echo "  make clean            - Remove generated files and volumes"
	@echo "  make test-integration - Run integration tests"