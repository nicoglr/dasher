.PHONY: build test lint smoke

build:
	go build ./...

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

smoke: build
	./smoke.sh
