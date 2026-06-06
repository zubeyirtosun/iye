package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ExistingFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := `
log_level: debug
tailer:
  paths:
    - /var/log/test/*.log
  poll_interval: 500ms
  max_line_size: 4096
  read_buffer_size: 8192
buffer:
  enabled: true
  path: /tmp/iye-buf
  max_size_bytes: 1048576
transport:
  enabled: false
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level 'debug', got '%s'", cfg.LogLevel)
	}
	if len(cfg.Tailer.Paths) != 1 || cfg.Tailer.Paths[0] != "/var/log/test/*.log" {
		t.Errorf("Unexpected paths: %v", cfg.Tailer.Paths)
	}
	if !cfg.Buffer.Enabled {
		t.Error("Expected buffer enabled")
	}
	if cfg.Transport.Enabled {
		t.Error("Expected transport disabled")
	}
}

func TestLoad_NonExistentFile(t *testing.T) {
	cfg, err := Load("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("Expected no error for non-existent file: %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected default config for non-existent file")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(cfgPath, []byte("invalid: yaml: : : broken"), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("Expected error for invalid YAML")
	}
}

func TestLoadOrDefault_SpecifiedPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "custom.yaml")
	content := `
log_level: error
tailer:
  paths:
    - /custom/path/*.log
  poll_interval: 1s
  max_line_size: 1024
  read_buffer_size: 4096
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	cfg, err := LoadOrDefault(cfgPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("Expected log_level 'error', got '%s'", cfg.LogLevel)
	}
}

func TestLoadOrDefault_DefaultFallback(t *testing.T) {
	cfg, err := LoadOrDefault()
	if err != nil {
		t.Fatalf("Expected no error: %v", err)
	}
	if cfg == nil {
		t.Fatal("Expected default config")
	}
	if !cfg.Masker.Enabled {
		t.Error("Expected masker enabled by default")
	}
}

func TestSave(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "saved.yaml")

	cfg, err := LoadOrDefault()
	if err != nil {
		t.Fatalf("Failed to get default config: %v", err)
	}

	if err := Save(cfg, cfgPath); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		t.Fatal("Config file was not created")
	}

	loaded, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Failed to reload saved config: %v", err)
	}
	if loaded.LogLevel != cfg.LogLevel {
		t.Errorf("Expected log_level '%s', got '%s'", cfg.LogLevel, loaded.LogLevel)
	}
}
