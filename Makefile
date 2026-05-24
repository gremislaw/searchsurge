.PHONY: up down build test test-race proto-gen proto-setup bench logs lint clean

up:
	docker compose up -d

down:
	docker compose down --remove-orphans

build:
	docker compose build --no-cache

logs:
	docker compose logs -f searchsurge-master searchsurge-slave

test:
	go test ./... -v -count=1

test-race:
	go test ./... -v -race -count=1

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
	k6 run scripts/loadtest.js

clean:
	rm -rf pb/ googleapis/
	docker compose down -v --remove-orphans