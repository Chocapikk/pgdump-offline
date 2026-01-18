package pgdump

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDetectDataDir(t *testing.T) {
	// Create a fake PostgreSQL data directory
	tmpDir := t.TempDir()
	globalDir := filepath.Join(tmpDir, "global")
	if err := os.MkdirAll(globalDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create fake pg_database file
	pgDatabase := filepath.Join(globalDir, "1262")
	if err := os.WriteFile(pgDatabase, make([]byte, 8192), 0644); err != nil {
		t.Fatal(err)
	}

	// Test with PGDATA
	os.Setenv("PGDATA", tmpDir)
	defer os.Unsetenv("PGDATA")

	detected := DetectDataDir()
	if detected != tmpDir {
		t.Errorf("Expected %s, got %s", tmpDir, detected)
	}
}

func TestDetectAllDataDirs(t *testing.T) {
	// Create a fake PostgreSQL data directory
	tmpDir := t.TempDir()
	globalDir := filepath.Join(tmpDir, "global")
	if err := os.MkdirAll(globalDir, 0755); err != nil {
		t.Fatal(err)
	}

	pgDatabase := filepath.Join(globalDir, "1262")
	if err := os.WriteFile(pgDatabase, make([]byte, 8192), 0644); err != nil {
		t.Fatal(err)
	}

	os.Setenv("PGDATA", tmpDir)
	defer os.Unsetenv("PGDATA")

	dirs := DetectAllDataDirs()
	if len(dirs) == 0 {
		t.Error("Expected at least one directory")
	}

	found := false
	for _, d := range dirs {
		if d == tmpDir {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find %s in %v", tmpDir, dirs)
	}
}

func TestGetDataDirCandidates(t *testing.T) {
	candidates := getDataDirCandidates()
	if len(candidates) == 0 {
		t.Error("Expected at least one candidate path")
	}

	// Check platform-specific paths exist
	switch runtime.GOOS {
	case "linux":
		found := false
		for _, c := range candidates {
			if c == "/var/lib/postgresql/data" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected /var/lib/postgresql/data in Linux candidates")
		}

	case "darwin":
		found := false
		for _, c := range candidates {
			if c == "/usr/local/var/postgres" || c == "/opt/homebrew/var/postgres" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected Homebrew path in macOS candidates")
		}

	case "windows":
		found := false
		for _, c := range candidates {
			if filepath.Base(filepath.Dir(c)) == "PostgreSQL" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected Program Files path in Windows candidates")
		}
	}
}

func TestIsValidDataDir(t *testing.T) {
	// Invalid path
	if isValidDataDir("/nonexistent/path") {
		t.Error("Expected false for nonexistent path")
	}

	// Valid fake path
	tmpDir := t.TempDir()
	globalDir := filepath.Join(tmpDir, "global")
	os.MkdirAll(globalDir, 0755)
	os.WriteFile(filepath.Join(globalDir, "1262"), make([]byte, 8192), 0644)

	if !isValidDataDir(tmpDir) {
		t.Error("Expected true for valid data dir")
	}

	// Empty file should fail
	tmpDir2 := t.TempDir()
	globalDir2 := filepath.Join(tmpDir2, "global")
	os.MkdirAll(globalDir2, 0755)
	os.WriteFile(filepath.Join(globalDir2, "1262"), []byte{}, 0644)

	if isValidDataDir(tmpDir2) {
		t.Error("Expected false for empty pg_database file")
	}
}
