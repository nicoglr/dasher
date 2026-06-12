.PHONY: build test lint smoke

build:
	go build ./...

test:
	go test ./... -v -count=1

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...

smoke: build
	./smoke.sh
