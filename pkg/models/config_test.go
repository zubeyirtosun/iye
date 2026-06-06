package models

import (
	"testing"
	"time"
)

func TestDefaultConfig_Valid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Default config should be valid: %v", err)
	}
}

func TestTailerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     TailerConfig
		wantErr bool
	}{
		{"valid", TailerConfig{Paths: []string{"/var/log"}, PollInterval: time.Second, MaxLineSize: 1024, ReadBufferSize: 4096}, false},
		{"no paths", TailerConfig{Paths: nil, PollInterval: time.Second, MaxLineSize: 1024, ReadBufferSize: 4096}, true},
		{"empty paths", TailerConfig{Paths: []string{}, PollInterval: time.Second, MaxLineSize: 1024, ReadBufferSize: 4096}, true},
		{"zero poll interval", TailerConfig{Paths: []string{"/var/log"}, PollInterval: 0, MaxLineSize: 1024, ReadBufferSize: 4096}, true},
		{"zero max line size", TailerConfig{Paths: []string{"/var/log"}, PollInterval: time.Second, MaxLineSize: 0, ReadBufferSize: 4096}, true},
		{"zero read buffer", TailerConfig{Paths: []string{"/var/log"}, PollInterval: time.Second, MaxLineSize: 1024, ReadBufferSize: 0}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestSamplingConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     SamplingConfig
		wantErr bool
	}{
		{"valid", SamplingConfig{ErrorThreshold: 0.05, WindowSize: time.Minute, CooldownPeriod: time.Minute, MinSampleRate: 0.1, MaxSampleRate: 1.0}, false},
		{"negative error threshold", SamplingConfig{ErrorThreshold: -0.1, WindowSize: time.Minute, CooldownPeriod: time.Minute, MinSampleRate: 0.1, MaxSampleRate: 1.0}, true},
		{"error threshold > 1", SamplingConfig{ErrorThreshold: 1.5, WindowSize: time.Minute, CooldownPeriod: time.Minute, MinSampleRate: 0.1, MaxSampleRate: 1.0}, true},
		{"zero window", SamplingConfig{ErrorThreshold: 0.05, WindowSize: 0, CooldownPeriod: time.Minute, MinSampleRate: 0.1, MaxSampleRate: 1.0}, true},
		{"min > max", SamplingConfig{ErrorThreshold: 0.05, WindowSize: time.Minute, CooldownPeriod: time.Minute, MinSampleRate: 0.8, MaxSampleRate: 0.5}, true},
		{"negative min rate", SamplingConfig{ErrorThreshold: 0.05, WindowSize: time.Minute, CooldownPeriod: time.Minute, MinSampleRate: -0.1, MaxSampleRate: 1.0}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestBufferConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     BufferConfig
		wantErr bool
	}{
		{"valid", BufferConfig{Path: "/tmp/buf", MaxSizeBytes: 1024}, false},
		{"empty path", BufferConfig{Path: "", MaxSizeBytes: 1024}, true},
		{"zero max size", BufferConfig{Path: "/tmp/buf", MaxSizeBytes: 0}, true},
		{"negative max size", BufferConfig{Path: "/tmp/buf", MaxSizeBytes: -1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestTransportConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     TransportConfig
		wantErr bool
	}{
		{"valid", TransportConfig{Endpoint: "http://example.com", Type: "http", Compression: "zstd", BatchSize: 100, BatchTimeout: time.Second}, false},
		{"empty endpoint", TransportConfig{Endpoint: "", Type: "http", Compression: "zstd", BatchSize: 100, BatchTimeout: time.Second}, true},
		{"invalid type", TransportConfig{Endpoint: "http://example.com", Type: "grpc", Compression: "zstd", BatchSize: 100, BatchTimeout: time.Second}, true},
		{"invalid compression", TransportConfig{Endpoint: "http://example.com", Type: "http", Compression: "lz4", BatchSize: 100, BatchTimeout: time.Second}, true},
		{"zero batch size", TransportConfig{Endpoint: "http://example.com", Type: "http", Compression: "zstd", BatchSize: 0, BatchTimeout: time.Second}, true},
		{"tls missing key", TransportConfig{Endpoint: "http://example.com", Type: "http", Compression: "zstd", BatchSize: 100, BatchTimeout: time.Second, TLSCertFile: "/tmp/cert.pem"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_LogLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		wantErr bool
	}{
		{"debug", "debug", false},
		{"info", "info", false},
		{"warn", "warn", false},
		{"warning", "warning", false},
		{"error", "error", false},
		{"invalid", "trace", true},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.LogLevel = tt.level
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_SubConfigs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Sampling.Enabled = true
	cfg.Sampling.ErrorThreshold = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("Expected validation error for invalid sampling config")
	}

	cfg2 := DefaultConfig()
	cfg2.Buffer.Enabled = true
	cfg2.Buffer.Path = ""
	if err := cfg2.Validate(); err == nil {
		t.Error("Expected validation error for invalid buffer config")
	}

	cfg3 := DefaultConfig()
	cfg3.Transport.Enabled = true
	cfg3.Transport.Endpoint = ""
	if err := cfg3.Validate(); err == nil {
		t.Error("Expected validation error for invalid transport config")
	}
}
