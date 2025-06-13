# Version information
VERSION ?= $(shell grep -E '^\s*Version\s*=' pkg/version/version.go | cut -d'"' -f2)
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GO_VERSION ?= $(shell go version | cut -d' ' -f3)

# Build flags
LDFLAGS := -ldflags "\
	-X github.com/KevoDB/kevo/pkg/version.Version=$(VERSION) \
	-X github.com/KevoDB/kevo/pkg/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/KevoDB/kevo/pkg/version.BuildTime=$(BUILD_TIME) \
	-X github.com/KevoDB/kevo/pkg/version.GoVersion=$(GO_VERSION)"

.PHONY: all build clean test version

all: build

build:
	go build $(LDFLAGS) -o kevo ./cmd/kevo

clean:
	rm -f kevo

test:
	go test ./...

# Show version information
version:
	@echo "Version: $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(GO_VERSION)"
