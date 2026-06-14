.PHONY: build test lint smoke test-integration

build:
	go build ./...

test:
	go test -race ./... -v -count=1

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...

smoke: build
	./smoke.sh

test-integration:
	./scripts/run-integration-tests.sh
