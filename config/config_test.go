package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fb-tunnel/fb-tunnel-go/config"
)

// ── ClientConfig tests ────────────────────────────────────────────────────────

func TestLoadClientConfigDefaults(t *testing.T) {
	content := `
firebase_url    = "https://test-project-default-rtdb.firebaseio.com"
firebase_secret = "test-secret"
`
	path := writeTempTOML(t, content)
	cfg, err := config.LoadClientConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Listen != "127.0.0.1:1080" {
		t.Errorf("listen default: got %q, want %q", cfg.Listen, "127.0.0.1:1080")
	}
	if cfg.BatchIntervalMS != 50 {
		t.Errorf("batch_interval_ms default: got %d, want 50", cfg.BatchIntervalMS)
	}
	if cfg.BatchMaxBytes != 32*1024 {
		t.Errorf("batch_max_bytes default: got %d, want %d", cfg.BatchMaxBytes, 32*1024)
	}
	if cfg.RetryLimit != 5 {
		t.Errorf("retry_limit default: got %d, want 5", cfg.RetryLimit)
	}
}

func TestLoadClientConfigExplicitValues(t *testing.T) {
	content := `
firebase_url       = "https://my-project-default-rtdb.firebaseio.com"
firebase_secret    = "mysecret"
listen             = "0.0.0.0:9090"
batch_interval_ms  = 100
batch_max_bytes    = 65536
retry_limit        = 3
`
	path := writeTempTOML(t, content)
	cfg, err := config.LoadClientConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.FirebaseURL != "https://my-project-default-rtdb.firebaseio.com" {
		t.Errorf("firebase_url: got %q", cfg.FirebaseURL)
	}
	if cfg.FirebaseSecret != "mysecret" {
		t.Errorf("firebase_secret: got %q", cfg.FirebaseSecret)
	}
	if cfg.Listen != "0.0.0.0:9090" {
		t.Errorf("listen: got %q", cfg.Listen)
	}
	if cfg.BatchIntervalMS != 100 {
		t.Errorf("batch_interval_ms: got %d", cfg.BatchIntervalMS)
	}
	if cfg.BatchMaxBytes != 65536 {
		t.Errorf("batch_max_bytes: got %d", cfg.BatchMaxBytes)
	}
	if cfg.RetryLimit != 3 {
		t.Errorf("retry_limit: got %d", cfg.RetryLimit)
	}
}

func TestLoadClientConfigFileNotFound(t *testing.T) {
	_, err := config.LoadClientConfig("/nonexistent/path/client.toml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadClientConfigInvalidTOML(t *testing.T) {
	content := `this is not valid toml {{{{`
	path := writeTempTOML(t, content)
	_, err := config.LoadClientConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid TOML, got nil")
	}
}

// ── ServerConfig tests ────────────────────────────────────────────────────────

func TestLoadServerConfigDefaults(t *testing.T) {
	content := `
firebase_url    = "https://test-default-rtdb.firebaseio.com"
firebase_secret = "server-secret"
`
	path := writeTempTOML(t, content)
	cfg, err := config.LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.PollIntervalMS != 200 {
		t.Errorf("poll_interval_ms default: got %d, want 200", cfg.PollIntervalMS)
	}
	if cfg.SessionTimeoutS != 300 {
		t.Errorf("session_timeout_s default: got %d, want 300", cfg.SessionTimeoutS)
	}
	if cfg.RetryLimit != 5 {
		t.Errorf("retry_limit default: got %d, want 5", cfg.RetryLimit)
	}
}

func TestLoadServerConfigExplicitValues(t *testing.T) {
	content := `
firebase_url      = "https://my-project-default-rtdb.firebaseio.com"
firebase_secret   = "srvscrt"
poll_interval_ms  = 500
session_timeout_s = 600
retry_limit       = 10
`
	path := writeTempTOML(t, content)
	cfg, err := config.LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.PollIntervalMS != 500 {
		t.Errorf("poll_interval_ms: got %d", cfg.PollIntervalMS)
	}
	if cfg.SessionTimeoutS != 600 {
		t.Errorf("session_timeout_s: got %d", cfg.SessionTimeoutS)
	}
	if cfg.RetryLimit != 10 {
		t.Errorf("retry_limit: got %d", cfg.RetryLimit)
	}
}

func TestLoadServerConfigFileNotFound(t *testing.T) {
	_, err := config.LoadServerConfig("/nonexistent/path/server.toml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// ── ApplyDefaults ─────────────────────────────────────────────────────────────

func TestClientConfigApplyDefaultsDoesNotOverrideExplicit(t *testing.T) {
	cfg := &config.ClientConfig{
		Listen:          "0.0.0.0:8080",
		BatchIntervalMS: 10,
		BatchMaxBytes:   1024,
		RetryLimit:      1,
	}
	cfg.ApplyDefaults()

	if cfg.Listen != "0.0.0.0:8080" {
		t.Errorf("ApplyDefaults should not overwrite Listen, got %q", cfg.Listen)
	}
	if cfg.BatchIntervalMS != 10 {
		t.Errorf("ApplyDefaults should not overwrite BatchIntervalMS, got %d", cfg.BatchIntervalMS)
	}
}

func TestServerConfigApplyDefaultsDoesNotOverrideExplicit(t *testing.T) {
	cfg := &config.ServerConfig{
		PollIntervalMS:  50,
		SessionTimeoutS: 60,
		RetryLimit:      2,
	}
	cfg.ApplyDefaults()

	if cfg.PollIntervalMS != 50 {
		t.Errorf("ApplyDefaults should not overwrite PollIntervalMS, got %d", cfg.PollIntervalMS)
	}
	if cfg.SessionTimeoutS != 60 {
		t.Errorf("ApplyDefaults should not overwrite SessionTimeoutS, got %d", cfg.SessionTimeoutS)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeTempTOML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}
