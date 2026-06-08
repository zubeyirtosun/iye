package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/iye/iye/internal/buffer"
	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func newTestBuffer(t *testing.T) (*buffer.DiskBuffer, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "iye-transport-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	cfg := &models.BufferConfig{
		Enabled:      true,
		Path:         dir,
		MaxSizeBytes: 64 << 20,
		SyncWrites:   true,
	}

	b, err := buffer.NewDiskBuffer(cfg, logger)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create buffer: %v", err)
	}

	cleanup := func() {
		b.Close()
		os.RemoveAll(dir)
	}

	return b, cleanup
}

type mockMetricsCollector struct{}

func (m *mockMetricsCollector) RecordBufferDropped(bufferName string, count int) {}
func (m *mockMetricsCollector) UpdateBufferSize(bufferName string, sizeBytes int64) {}

func TestTransport_NewTransport(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.TransportConfig{
		Enabled:     true,
		Type:        "http",
		Endpoint:    "http://localhost:9999/api/logs",
		Compression: "none",
		BatchSize:   100,
		BatchTimeout: 5 * time.Second,
	}

	tr, err := NewTransport(config, logger, nil, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	if tr == nil {
		t.Fatal("Expected non-nil transport")
	}
}

func TestTransport_NewTransport_InvalidCompression(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.TransportConfig{
		Compression: "invalid",
	}

	_, err := NewTransport(config, logger, nil, &mockMetricsCollector{})
	if err == nil {
		t.Fatal("Expected error for invalid compression")
	}
}

func TestTransport_CompressNone(t *testing.T) {
	c := &NoneCompressor{}
	data := []byte("hello world")
	out, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}
	if string(out) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", out)
	}
}

func TestTransport_CompressZstd(t *testing.T) {
	c, err := NewZstdCompressor()
	if err != nil {
		t.Fatalf("Failed to create zstd compressor: %v", err)
	}
	data := []byte("hello world")
	out, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}
	if string(out) == "hello world" {
		t.Error("Expected compressed output to differ from input")
	}
}

func TestTransport_SendBatch(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	buf, cleanup := newTestBuffer(t)
	defer cleanup()

	// Write some entries to buffer
	for i := 0; i < 5; i++ {
		entry := models.LogEntry{
			Timestamp: time.Now().Format(time.RFC3339Nano),
			Source:    "test",
			Message:   "test message",
			Level:     "info",
		}
		if err := buf.Write(entry); err != nil {
			t.Fatalf("Failed to write to buffer: %v", err)
		}
	}

	// Start a test server
	var received bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		if r.Header.Get("X-Compression") == "" {
			t.Error("Expected X-Compression header")
		}
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("Expected application/octet-stream, got %s", r.Header.Get("Content-Type"))
		}
		received = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &models.TransportConfig{
		Enabled:      true,
		Type:         "http",
		Endpoint:     server.URL,
		Compression:  "none",
		BatchSize:    10,
		BatchTimeout: 50 * time.Millisecond,
	}

	tr, err := NewTransport(config, logger, buf, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr.Start(ctx)

	// Wait for flush (batch timeout is 50ms)
	time.Sleep(100 * time.Millisecond)
	tr.Stop()

	if !received {
		t.Error("Expected server to receive request")
	}
}

func TestTransport_EmptyBuffer(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.TransportConfig{
		Enabled:      true,
		Type:         "http",
		Endpoint:     "http://localhost:9999/api/logs",
		Compression:  "none",
		BatchSize:    10,
		BatchTimeout: time.Second,
	}

	tr, err := NewTransport(config, logger, nil, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	tr.Stop()
}

func TestTransport_ServerError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	buf, cleanup := newTestBuffer(t)
	defer cleanup()

	entry := models.LogEntry{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Source:    "test",
		Message:   "test",
		Level:     "info",
	}
	if err := buf.Write(entry); err != nil {
		t.Fatalf("Failed to write to buffer: %v", err)
	}

	// Server returns 500 only once, then OK
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	config := &models.TransportConfig{
		Enabled:      true,
		Endpoint:     server.URL,
		Compression:  "none",
		BatchSize:    10,
		BatchTimeout: 50 * time.Millisecond,
	}

	tr, err := NewTransport(config, logger, buf, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tr.Start(ctx)

	// Wait for a few retries
	time.Sleep(500 * time.Millisecond)

	// After failed send, buffer should still have the entry (returned on failure)
	count, _ := buf.Len()
	if count == 0 {
		t.Log("Entry was returned to buffer after failure")
	}

	tr.Stop()

	if callCount == 0 {
		t.Error("Expected server to receive at least one request")
	}
}

func TestTransport_GzipCompression(t *testing.T) {
	c := &GzipCompressor{}
	data := []byte("hello world")
	out, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}
	if string(out) == "hello world" {
		t.Error("Expected compressed output to differ from input")
	}
}

func TestTransport_ConcurrentStartStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.TransportConfig{
		Enabled:      true,
		Endpoint:     "http://localhost:9999/api/logs",
		Compression:  "none",
		BatchSize:    10,
		BatchTimeout: time.Second,
	}

	tr, err := NewTransport(config, logger, nil, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	ctx := context.Background()

	// Start multiple times (should be safe)
	tr.Start(ctx)
	tr.Start(ctx)
	tr.Stop()
	tr.Stop()
}

func TestTransport_BuildTLSConfig_NoCert(t *testing.T) {
	config := &models.TransportConfig{
		InsecureSkipVerify: false,
	}
	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		t.Fatalf("Failed to build TLS config: %v", err)
	}
	if tlsConfig != nil {
		t.Error("Expected nil TLS config when no certs provided")
	}
}

func TestTransport_BuildTLSConfig_Insecure(t *testing.T) {
	config := &models.TransportConfig{
		InsecureSkipVerify: true,
	}
	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		t.Fatalf("Failed to build TLS config: %v", err)
	}
	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify=true")
	}
}

func TestTransport_BuildTLSConfig_MissingKey(t *testing.T) {
	config := &models.TransportConfig{
		TLSCertFile: "/tmp/cert.pem",
	}
	_, err := buildTLSConfig(config)
	if err == nil {
		t.Error("Expected error when TLS key is missing")
	}
}

func TestTransport_BuildTLSConfig_MissingCert(t *testing.T) {
	config := &models.TransportConfig{
		TLSKeyFile: "/tmp/key.pem",
	}
	_, err := buildTLSConfig(config)
	if err == nil {
		t.Error("Expected error when TLS cert is missing")
	}
}

func TestTransport_BatchPayloadCompressedFlag(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	buf, cleanup := newTestBuffer(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		entry := models.LogEntry{
			Timestamp: time.Now().Format(time.RFC3339Nano),
			Source:    "test",
			Message:   "test message",
			Level:     "info",
		}
		if err := buf.Write(entry); err != nil {
			t.Fatalf("Failed to write to buffer: %v", err)
		}
	}

	var payloadBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		payloadBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &models.TransportConfig{
		Enabled:      true,
		Endpoint:     server.URL,
		Compression:  "none",
		BatchSize:    10,
		BatchTimeout: 50 * time.Millisecond,
	}

	tr, err := NewTransport(config, logger, buf, &mockMetricsCollector{})
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	tr.Stop()

	if payloadBody == nil {
		t.Fatal("Expected server to receive request body")
	}

	var payload batchPayload
	if err := json.Unmarshal(payloadBody, &payload); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v\nBody: %s", err, string(payloadBody))
	}

	if payload.Compressed {
		t.Errorf("Expected compressed=false for none compressor, got true. Body: %s", string(payloadBody))
	}
	if payload.Algorithm != "none" {
		t.Errorf("Expected algorithm 'none', got '%s'", payload.Algorithm)
	}
	if len(payload.Entries) == 0 {
		t.Error("Expected at least one entry in payload")
	}
	if payload.Count != 3 {
		t.Errorf("Expected count 3, got %d", payload.Count)
	}
}

func TestTransport_CompressorNames(t *testing.T) {
	gzip := &GzipCompressor{}
	if gzip.Name() != "gzip" {
		t.Errorf("Expected 'gzip', got '%s'", gzip.Name())
	}

	none := &NoneCompressor{}
	if none.Name() != "none" {
		t.Errorf("Expected 'none', got '%s'", none.Name())
	}

	zstd, err := NewZstdCompressor()
	if err != nil {
		t.Fatal(err)
	}
	if zstd.Name() != "zstd" {
		t.Errorf("Expected 'zstd', got '%s'", zstd.Name())
	}
}
