package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

func DataDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("failed to resolve user config dir: %w", err)
	}

	return filepath.Join(base, "quacktel"), nil
}

// EnsureDir refuses wider modes rather than chmod, so a user's choice is never silently changed.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat directory %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	if mode := info.Mode().Perm(); mode&^0o700 != 0 {
		return fmt.Errorf("directory %s has mode %04o, want 0700 or stricter", path, mode)
	}

	return nil
}

// OTLPHeadersFile is shared so serve, env and doctor agree on one name and format.
const OTLPHeadersFile = "otlp-headers"

var OTLPHeadersFormat = regexp.MustCompile(`^authorization=Bearer ([0-9a-f]{64})\n$`)
