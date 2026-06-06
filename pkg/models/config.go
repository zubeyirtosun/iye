package models

import (
	"fmt"
	"time"
)

type LogLine struct {
	Timestamp  time.Time
	Source     string
	Content    string
	Raw        []byte
	Labels     map[string]string
	Severity   SeverityLevel
	Processed  bool
	Masked     bool
	Sampled    bool
}

type SeverityLevel int

const (
	SeverityUnknown SeverityLevel = iota
	SeverityDebug
	SeverityInfo
	SeverityWarn
	SeverityError
	SeverityFatal
	SeverityPanic
)

func (s SeverityLevel) String() string {
	switch s {
	case SeverityDebug:
		return "debug"
	case SeverityInfo:
		return "info"
	case SeverityWarn:
		return "warn"
	case SeverityError:
		return "error"
	case SeverityFatal:
		return "fatal"
	case SeverityPanic:
		return "panic"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Timestamp string            `json:"timestamp"`
	Source    string            `json:"source"`
	Message   string            `json:"message"`
	Level     string            `json:"level"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type TailerConfig struct {
	Paths           []string      `yaml:"paths" json:"paths"`
	PollInterval    time.Duration `yaml:"poll_interval" json:"poll_interval"`
	MaxLineSize     int           `yaml:"max_line_size" json:"max_line_size"`
	ReadBufferSize  int           `yaml:"read_buffer_size" json:"read_buffer_size"`
	FollowSymlinks  bool          `yaml:"follow_symlinks" json:"follow_symlinks"`
	FromBeginning   bool          `yaml:"from_beginning" json:"from_beginning"`
	IncludePattern  string        `yaml:"include_pattern" json:"include_pattern"`
	ExcludePattern  string        `yaml:"exclude_pattern" json:"exclude_pattern"`
}

type MaskerConfig struct {
	Enabled             bool     `yaml:"enabled" json:"enabled"`
	CustomPatterns      []string `yaml:"custom_patterns" json:"custom_patterns"`
	MaskReplacement     string   `yaml:"mask_replacement" json:"mask_replacement"`
	PreserveLength      bool     `yaml:"preserve_length" json:"preserve_length"`
}

type SamplingConfig struct {
	Enabled              bool          `yaml:"enabled" json:"enabled"`
	ErrorThreshold       float64       `yaml:"error_threshold" json:"error_threshold"`
	WindowSize           time.Duration `yaml:"window_size" json:"window_size"`
	CooldownPeriod       time.Duration `yaml:"cooldown_period" json:"cooldown_period"`
	MinSampleRate        float64       `yaml:"min_sample_rate" json:"min_sample_rate"`
	MaxSampleRate        float64       `yaml:"max_sample_rate" json:"max_sample_rate"`
	AnomalyWindowMinutes int           `yaml:"anomaly_window_minutes" json:"anomaly_window_minutes"`
}

type MetricsConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	ListenAddress  string        `yaml:"listen_address" json:"listen_address"`
	MetricsPath    string        `yaml:"metrics_path" json:"metrics_path"`
	ScrapeInterval time.Duration `yaml:"scrape_interval" json:"scrape_interval"`
	Buckets        []float64     `yaml:"buckets" json:"buckets"`
}

type BufferConfig struct {
	Enabled      bool          `yaml:"enabled" json:"enabled"`
	Path         string        `yaml:"path" json:"path"`
	MaxSizeBytes int64         `yaml:"max_size_bytes" json:"max_size_bytes"`
	SyncWrites   bool          `yaml:"sync_writes" json:"sync_writes"`
}

type TransportConfig struct {
	Enabled       bool   `yaml:"enabled" json:"enabled"`
	Type          string `yaml:"type" json:"type"`
	Endpoint      string `yaml:"endpoint" json:"endpoint"`
	Compression   string `yaml:"compression" json:"compression"`
	BatchSize     int    `yaml:"batch_size" json:"batch_size"`
	BatchTimeout  time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	TLSCertFile   string `yaml:"tls_cert_file" json:"tls_cert_file"`
	TLSKeyFile    string `yaml:"tls_key_file" json:"tls_key_file"`
	InsecureSkipVerify bool `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
}

type Config struct {
	Tailer    TailerConfig    `yaml:"tailer" json:"tailer"`
	Masker    MaskerConfig    `yaml:"masker" json:"masker"`
	Sampling  SamplingConfig  `yaml:"sampling" json:"sampling"`
	Metrics   MetricsConfig   `yaml:"metrics" json:"metrics"`
	Buffer    BufferConfig    `yaml:"buffer" json:"buffer"`
	Transport TransportConfig `yaml:"transport" json:"transport"`
	LogLevel  string          `yaml:"log_level" json:"log_level"`
}

func DefaultConfig() *Config {
	return &Config{
		Tailer: TailerConfig{
			Paths:          []string{"/var/log/pods/**/*.log"},
			PollInterval:   100 * time.Millisecond,
			MaxLineSize:    1024 * 1024,
			ReadBufferSize: 64 * 1024,
			FollowSymlinks: true,
			FromBeginning:  false,
		},
		Masker: MaskerConfig{
			Enabled:         true,
			MaskReplacement: "[MASKED]",
			PreserveLength:  false,
		},
		Sampling: SamplingConfig{
			Enabled:              true,
			ErrorThreshold:       0.05,
			WindowSize:           60 * time.Second,
			CooldownPeriod:       5 * time.Minute,
			MinSampleRate:        0.01,
			MaxSampleRate:        1.0,
			AnomalyWindowMinutes: 5,
		},
		Metrics: MetricsConfig{
			Enabled:       true,
			ListenAddress: ":9090",
			MetricsPath:   "/metrics",
			ScrapeInterval: 15 * time.Second,
			Buckets:       []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		Buffer: BufferConfig{
			Enabled:      true,
			Path:         "/var/lib/iye/buffer",
			MaxSizeBytes: 512 * 1024 * 1024,
			SyncWrites:   false,
		},
		Transport: TransportConfig{
			Enabled:       false,
			Type:          "http",
			Compression:   "zstd",
			BatchSize:     1000,
			BatchTimeout:  5 * time.Second,
			InsecureSkipVerify: false,
		},
		LogLevel: "info",
	}
}

func (c *Config) Validate() error {
	if err := c.Tailer.Validate(); err != nil {
		return fmt.Errorf("tailer: %w", err)
	}
	if err := c.Metrics.Validate(); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	if c.Sampling.Enabled {
		if err := c.Sampling.Validate(); err != nil {
			return fmt.Errorf("sampling: %w", err)
		}
	}
	if c.Buffer.Enabled {
		if err := c.Buffer.Validate(); err != nil {
			return fmt.Errorf("buffer: %w", err)
		}
	}
	if c.Transport.Enabled {
		if err := c.Transport.Validate(); err != nil {
			return fmt.Errorf("transport: %w", err)
		}
	}
	if c.LogLevel != "" {
		switch c.LogLevel {
		case "debug", "info", "warn", "warning", "error":
		default:
			return fmt.Errorf("invalid log_level: %s", c.LogLevel)
		}
	}
	return nil
}

func (c *TailerConfig) Validate() error {
	if len(c.Paths) == 0 {
		return fmt.Errorf("at least one path is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll_interval must be positive")
	}
	if c.MaxLineSize <= 0 {
		return fmt.Errorf("max_line_size must be positive")
	}
	if c.ReadBufferSize <= 0 {
		return fmt.Errorf("read_buffer_size must be positive")
	}
	return nil
}

func (c *SamplingConfig) Validate() error {
	if c.ErrorThreshold < 0 || c.ErrorThreshold > 1 {
		return fmt.Errorf("error_threshold must be between 0 and 1")
	}
	if c.WindowSize <= 0 {
		return fmt.Errorf("window_size must be positive")
	}
	if c.CooldownPeriod <= 0 {
		return fmt.Errorf("cooldown_period must be positive")
	}
	if c.MinSampleRate < 0 || c.MinSampleRate > 1 {
		return fmt.Errorf("min_sample_rate must be between 0 and 1")
	}
	if c.MaxSampleRate < 0 || c.MaxSampleRate > 1 {
		return fmt.Errorf("max_sample_rate must be between 0 and 1")
	}
	if c.MinSampleRate > c.MaxSampleRate {
		return fmt.Errorf("min_sample_rate cannot exceed max_sample_rate")
	}
	return nil
}

func (c *MetricsConfig) Validate() error {
	if c.ListenAddress == "" {
		return fmt.Errorf("listen_address is required")
	}
	if c.MetricsPath == "" {
		return fmt.Errorf("metrics_path is required")
	}
	if c.ScrapeInterval <= 0 {
		return fmt.Errorf("scrape_interval must be positive")
	}
	return nil
}

func (c *BufferConfig) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("path is required")
	}
	if c.MaxSizeBytes <= 0 {
		return fmt.Errorf("max_size_bytes must be positive")
	}
	return nil
}

func (c *TransportConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	switch c.Type {
	case "http", "https":
	default:
		return fmt.Errorf("unsupported transport type: %s", c.Type)
	}
	switch c.Compression {
	case "none", "gzip", "zstd", "":
	default:
		return fmt.Errorf("unsupported compression: %s", c.Compression)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}
	if c.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be positive")
	}
	if c.TLSCertFile != "" || c.TLSKeyFile != "" {
		if c.TLSCertFile == "" || c.TLSKeyFile == "" {
			return fmt.Errorf("both tls_cert_file and tls_key_file must be provided")
		}
	}
	return nil
}