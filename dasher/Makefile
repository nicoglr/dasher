.PHONY: build test lint up down

build:
	go build ./...

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

up:
	docker compose -f ../docker-compose.yml up -d

down:
	docker compose -f ../docker-compose.yml down
