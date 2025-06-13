# Version Package

This package provides centralized version management for Kevo.

## Usage

### Getting the version in code:

```go
import "github.com/KevoDB/kevo/pkg/version"

// Get simple version string
v := version.GetVersion() // "1.4.0"

// Get full version with build info
full := version.GetFullVersion() // "1.4.0 (commit: abc123, built: 2024-01-01_12:00:00)"

// Get all version info as struct
info := version.GetInfo()
fmt.Printf("Version: %s\n", info.Version)
fmt.Printf("Git Commit: %s\n", info.GitCommit)
fmt.Printf("Build Time: %s\n", info.BuildTime)
fmt.Printf("Go Version: %s\n", info.GoVersion)
```

### Building with version injection:

```bash
# Using make (recommended)
make build

# Using go build directly
go build -ldflags "\
  -X github.com/KevoDB/kevo/pkg/version.Version=1.4.1 \
  -X github.com/KevoDB/kevo/pkg/version.GitCommit=$(git rev-parse --short HEAD) \
  -X github.com/KevoDB/kevo/pkg/version.BuildTime=$(date -u '+%Y-%m-%d_%H:%M:%S') \
  -X github.com/KevoDB/kevo/pkg/version.GoVersion=$(go version | cut -d' ' -f3)" \
  ./cmd/kevo
```

### Updating the version:

To update the version, edit the `Version` variable in `version.go`:

```go
var (
    Version = "1.4.1"  // Update this line
    ...
)
```

The build system will automatically pick up the new version.