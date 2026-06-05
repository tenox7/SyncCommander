BINARY  := sc
PACKAGE := .
DIST    := dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build install run test release check clean

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) $(PACKAGE)

install:
	CGO_ENABLED=0 go install -trimpath -ldflags '$(LDFLAGS)' $(PACKAGE)

run: build
	./$(BINARY)

test:
	go test ./...

release:
	goreleaser release --snapshot --clean

check:
	goreleaser check

clean:
	rm -rf $(BINARY) $(DIST)
