VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-s -w -X github.com/depfloy/dpm/internal/daemon.Version=$(VERSION) -X main.version=$(VERSION)"

.PHONY: build build-linux clean test lint install

# Build for current platform
build:
	go build $(LDFLAGS) -o bin/dpm ./cmd/dpm
	go build $(LDFLAGS) -o bin/dpmd ./cmd/dpmd

# Build for Linux (production)
build-linux:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/dpm-linux-amd64 ./cmd/dpm
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/dpmd-linux-amd64 ./cmd/dpmd
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o bin/dpm-linux-arm64 ./cmd/dpm
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o bin/dpmd-linux-arm64 ./cmd/dpmd

# Generate checksums
checksums:
	cd bin && sha256sum dpm-linux-* dpmd-linux-* > checksums.txt

# Run tests
test:
	go test -v -race ./...

# Run linter
lint:
	golangci-lint run ./...

# Install locally (for development)
install: build
	sudo cp bin/dpm /usr/local/bin/dpm
	sudo cp bin/dpmd /usr/local/bin/dpmd

# Clean build artifacts
clean:
	rm -rf bin/

# Release build (for CI)
release: build-linux checksums
	@echo "Release artifacts in bin/"
	@ls -la bin/
