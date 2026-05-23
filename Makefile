.PHONY: up down build test test-race proto-gen bench logs

up:
	docker compose up -d

down:
	docker compose down --remove-orphans

build:
	docker compose build --no-cache

test:
	go test ./... -v -count=1

test-race:
	go test ./... -v -race -count=1

proto-gen:
	protoc --go_out=. --go-grpc_out=. --grpc-gateway_out=. \
	  --go_opt=paths=source_relative \
	  --go-grpc_opt=paths=source_relative \
	  --grpc-gateway_opt=paths=source_relative \
	  proto/api.proto

bench:
	k6 run scripts/loadtest.js

logs:
	docker compose logs -f searchsurge-master searchsurge-slave