package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/iye/iye/internal/buffer"
	"github.com/iye/iye/pkg/models"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Name() string
}

type GzipCompressor struct{}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *GzipCompressor) Name() string { return "gzip" }

type ZstdCompressor struct {
	encoder *zstd.Encoder
}

func NewZstdCompressor() (*ZstdCompressor, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return &ZstdCompressor{encoder: enc}, nil
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, nil), nil
}

func (z *ZstdCompressor) Name() string { return "zstd" }

type NoneCompressor struct{}

func (n *NoneCompressor) Compress(data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (n *NoneCompressor) Name() string { return "none" }

type batchEntry struct {
	Timestamp string            `json:"ts"`
	Source    string            `json:"src"`
	Message   string            `json:"msg"`
	Level     string            `json:"lvl"`
	Labels    map[string]string `json:"lbl,omitempty"`
}

type batchPayload struct {
	Entries    []batchEntry `json:"entries"`
	Count      int          `json:"count"`
	Compressed bool         `json:"compressed"`
	Algorithm  string       `json:"algorithm"`
}

type Transport struct {
	config     *models.TransportConfig
	logger     *zap.Logger
	buf        *buffer.DiskBuffer
	client     *http.Client
	compressor Compressor
	metrics    MetricsCollector

	mu          sync.Mutex
	running     bool
	stopCh      chan struct{}
	stopped     chan struct{}
	batch       []batchEntry
	batchSize   int
	lastSend    time.Time
	consecutive int
}

type MetricsCollector interface {
	RecordBufferDropped(bufferName string, count int)
	UpdateBufferSize(bufferName string, sizeBytes int64)
}

func NewTransport(config *models.TransportConfig, logger *zap.Logger, buf *buffer.DiskBuffer, metrics MetricsCollector) (*Transport, error) {
	var compressor Compressor
	switch config.Compression {
	case "zstd":
		c, err := NewZstdCompressor()
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd compressor: %w", err)
		}
		compressor = c
	case "gzip":
		compressor = &GzipCompressor{}
	case "none", "":
		compressor = &NoneCompressor{}
	default:
		return nil, fmt.Errorf("unsupported compression: %s", config.Compression)
	}

	t := &Transport{
		config:     config,
		logger:     logger.Named("transport"),
		buf:        buf,
		compressor: compressor,
		metrics:    metrics,
		batchSize: config.BatchSize,
	}

	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	t.client = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			TLSClientConfig:     tlsConfig,
		},
	}

	t.stopCh = make(chan struct{})
	t.stopped = make(chan struct{})

	if t.batchSize <= 0 {
		t.batchSize = 1000
	}

	t.logger.Info("Transport initialized",
		zap.String("type", config.Type),
		zap.String("endpoint", config.Endpoint),
		zap.String("compression", compressor.Name()),
		zap.Int("batch_size", t.batchSize),
	)

	return t, nil
}

func (t *Transport) Start(ctx context.Context) {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.running = true
	t.mu.Unlock()

	go t.run(ctx)
}

func (t *Transport) Stop() {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return
	}
	t.running = false
	close(t.stopCh)
	t.mu.Unlock()

	<-t.stopped

	// Flush any remaining batch
	t.flushBatch()
}

func (t *Transport) run(ctx context.Context) {
	defer close(t.stopped)

	ticker := time.NewTicker(t.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.flushBatch()
			return
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.flushFromBuffer()
			if len(t.batch) > 0 {
				t.sendBatch()
			}
		}
	}
}

func (t *Transport) flushFromBuffer() {
	t.mu.Lock()
	consecutive := t.consecutive
	t.mu.Unlock()
	if consecutive > 10 {
		return
	}
	for {
		entries, err := t.buf.ReadBatch(t.batchSize)
		if err != nil {
			t.logger.Error("Failed to read from buffer", zap.Error(err))
			return
		}
		if len(entries) == 0 {
			return
		}

		batch := make([]batchEntry, len(entries))
		for i, e := range entries {
			batch[i] = batchEntry{
				Timestamp: e.Timestamp,
				Source:    e.Source,
				Message:   e.Message,
				Level:     e.Level,
				Labels:    e.Labels,
			}
		}

		t.mu.Lock()
		t.batch = append(t.batch, batch...)
		t.mu.Unlock()

		if err := t.buf.Commit(len(entries)); err != nil {
			t.logger.Error("Failed to commit buffer entries", zap.Error(err))
		}

		if t.metrics != nil {
			size, err := t.buf.Size()
			if err == nil {
				t.metrics.UpdateBufferSize("main", int64(size))
			}
		}

		if len(entries) < t.batchSize {
			return
		}
	}
}

func (t *Transport) sendBatch() {
	t.mu.Lock()
	if len(t.batch) == 0 {
		t.mu.Unlock()
		return
	}
	batch := t.batch
	t.batch = nil
	t.mu.Unlock()

	payload := batchPayload{
		Entries:    batch,
		Count:      len(batch),
		Compressed: true,
		Algorithm:  t.compressor.Name(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		t.logger.Error("Failed to marshal batch", zap.Error(err))
		return
	}

	compressed, err := t.compressor.Compress(jsonData)
	if err != nil {
		t.logger.Error("Failed to compress batch", zap.Error(err))
		return
	}

	if err := t.sendWithRetry(compressed); err != nil {
		t.logger.Error("Failed to send batch",
			zap.Int("count", len(batch)),
			zap.Int("consecutive_failures", t.consecutive),
			zap.Error(err),
		)
		if t.consecutive > 10 {
			t.logger.Warn("Too many consecutive failures, dropping batch to prevent buffer poisoning",
				zap.Int("count", len(batch)),
			)
			if t.metrics != nil {
				t.metrics.RecordBufferDropped("transport", len(batch))
			}
		} else {
			t.returnToBuffer(batch)
		}
	}
}

func (t *Transport) sendWithRetry(data []byte) error {
	var lastErr error
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-t.stopCh:
				return fmt.Errorf("send cancelled")
			}
		}

		req, err := http.NewRequest("POST", t.config.Endpoint, bytes.NewReader(data))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Compression", t.compressor.Name())
		req.Header.Set("User-Agent", "iye-transport/1.0")

		resp, err := t.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			t.consecutive = 0
			return nil
		}

		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	t.consecutive++
	return fmt.Errorf("send failed after %d retries: %w", maxRetries, lastErr)
}

func (t *Transport) returnToBuffer(entries []batchEntry) {
	for _, e := range entries {
		entry := models.LogEntry{
			Timestamp: e.Timestamp,
			Source:    e.Source,
			Message:   e.Message,
			Level:     e.Level,
			Labels:    e.Labels,
		}
		if err := t.buf.Write(entry); err != nil {
			t.logger.Error("Failed to return entry to buffer", zap.Error(err))
		}
	}
}

func (t *Transport) flushBatch() {
	t.mu.Lock()
	batch := t.batch
	t.batch = nil
	t.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	payload := batchPayload{
		Entries:    batch,
		Count:      len(batch),
		Compressed: true,
		Algorithm:  t.compressor.Name(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		t.logger.Error("Failed to marshal final batch", zap.Error(err))
		return
	}

	compressed, err := t.compressor.Compress(jsonData)
	if err != nil {
		t.logger.Error("Failed to compress final batch", zap.Error(err))
		return
	}

	if err := t.sendWithRetry(compressed); err != nil {
		t.logger.Error("Failed to send final batch, returning to buffer",
			zap.Int("count", len(batch)),
			zap.Error(err),
		)
		t.returnToBuffer(batch)
	}
}

func buildTLSConfig(config *models.TransportConfig) (*tls.Config, error) {
	if config.TLSCertFile == "" && config.TLSKeyFile == "" {
		if config.InsecureSkipVerify {
			return &tls.Config{InsecureSkipVerify: true}, nil
		}
		return nil, nil
	}

	if config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return nil, fmt.Errorf("both tls_cert_file and tls_key_file must be provided")
	}

	cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: config.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if !config.InsecureSkipVerify {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to load system cert pool: %w", err)
		}
		tlsConfig.RootCAs = rootCAs
	}

	return tlsConfig, nil
}
