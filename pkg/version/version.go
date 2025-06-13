// ABOUTME: Provides centralized version management for the Kevo storage engine
// ABOUTME: Supports both compile-time constant and build-time injection via ldflags

package version

// Version variables that can be overridden at build time using ldflags
// Example: go build -ldflags "-X github.com/KevoDB/kevo/pkg/version.Version=1.2.3"
var (
	// Version is the semantic version of Kevo
	Version = "1.4.1"

	// GitCommit is the git commit hash (set via ldflags)
	GitCommit = "unknown"

	// BuildTime is the build timestamp (set via ldflags)
	BuildTime = "unknown"

	// GoVersion is the Go version used to build (set via ldflags)
	GoVersion = "unknown"
)

// GetVersion returns the current version string
func GetVersion() string {
	return Version
}

// GetFullVersion returns a detailed version string including build information
func GetFullVersion() string {
	if GitCommit == "unknown" && BuildTime == "unknown" {
		// Simple version when build info not available
		return Version
	}
	return Version + " (commit: " + GitCommit + ", built: " + BuildTime + ")"
}

// Info contains all version information
type Info struct {
	Version   string
	GitCommit string
	BuildTime string
	GoVersion string
}

// GetInfo returns a struct with all version information
func GetInfo() Info {
	return Info{
		Version:   Version,
		GitCommit: GitCommit,
		BuildTime: BuildTime,
		GoVersion: GoVersion,
	}
}
