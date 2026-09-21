package main

import (
	"os"
	"path/filepath"
	"testing"
)

// CRI-272: the adapter persists the Copilot SDK session ID per adapter session
// so a respawned adapter process (post-OOM/crash) can resume the same
// conversation instead of starting cold. Round-trip + fallback coverage.
func TestPersistAndLoadSDKSessionID(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)

	if got := loadPersistedSDKSessionID("adapter-1"); got != "" {
		t.Fatalf("loadPersistedSDKSessionID on empty dir = %q, want empty", got)
	}

	persistSDKSessionID("adapter-1", "sdk-session-abc")

	if got := loadPersistedSDKSessionID("adapter-1"); got != "sdk-session-abc" {
		t.Fatalf("loadPersistedSDKSessionID = %q, want %q", got, "sdk-session-abc")
	}

	// A different adapter session must not see another's ID.
	if got := loadPersistedSDKSessionID("adapter-2"); got != "" {
		t.Fatalf("loadPersistedSDKSessionID(adapter-2) = %q, want empty", got)
	}
}

func TestPersistSDKSessionIDEmptyArgsNoop(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CRITERIA_HOME", dir)

	persistSDKSessionID("", "sdk-x")
	persistSDKSessionID("adapter-x", "")

	if _, err := os.Stat(filepath.Join(dir, ".copilot-adapter")); !os.IsNotExist(err) {
		t.Fatalf("state dir created despite empty args: %v", err)
	}
}

// CRI-272: HOME fallback when CRITERIA_HOME is unset.
func TestSDKSessionIDPathFallsBackToHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CRITERIA_HOME", "")
	t.Setenv("HOME", home)

	got := sdkSessionIDPath("adapter-h")
	want := filepath.Join(home, ".copilot-adapter", "sdk-sessions", "adapter-h.id")
	if got != want {
		t.Fatalf("sdkSessionIDPath = %q, want %q", got, want)
	}
}
