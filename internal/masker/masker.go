package masker

import (
	"regexp"
	"strings"
	"sync"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

type Masker struct {
	config       *models.MaskerConfig
	logger       *zap.Logger
	patterns     []*compiledPattern
	customRegex  []*regexp.Regexp
	mu           sync.RWMutex
	stats        MaskerStats
}

type compiledPattern struct {
	name       string
	regex      *regexp.Regexp
	replacement string
}

type MaskerStats struct {
	LinesProcessed  uint64
	PatternsMatched uint64
	BytesMasked     uint64
}

var defaultPatterns = []*compiledPattern{
	{
		name: "aws_access_key",
		regex: regexp.MustCompile(`(?i)(aws_access_key_id|aws_secret_access_key|aws_session_token)\s*[=:]\s*["']?([A-Z0-9/+=]{20,})["']?`),
		replacement: "$1=[MASKED]",
	},
	{
		name: "aws_secret_key",
		regex: regexp.MustCompile(`(?i)AKIA[0-9A-Z]{16}`),
		replacement: "[MASKED_AWS_KEY]",
	},
	{
		name: "gcp_api_key",
		regex: regexp.MustCompile(`(?i)(gcp|google)[-_]?(api[-_]?key|service[-_]?account)\s*[=:]\s*["']?([A-Za-z0-9_\-]{20,})["']?`),
		replacement: "$1=[MASKED]",
	},
	{
		name: "gcp_service_account",
		regex: regexp.MustCompile(`"type"\s*:\s*"service_account".*?"private_key"\s*:\s*"[^"]*"`),
		replacement: `"type":"service_account","private_key":"[MASKED]"`,
	},
	{
		name: "jwt_token",
		regex: regexp.MustCompile(`eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`),
		replacement: "[MASKED_JWT]",
	},
	{
		name: "bearer_token",
		regex: regexp.MustCompile(`(?i)(authorization|bearer)\s*[=:]\s*["']?\s*Bearer\s+([A-Za-z0-9_\-\.]+)`),
		replacement: "$1=Bearer [MASKED]",
	},
	{
		name: "api_key_generic",
		regex: regexp.MustCompile(`(?i)(api[-_]?key|apikey|api_secret|client_secret)\s*[=:]\s*["']?([A-Za-z0-9_\-]{20,})["']?`),
		replacement: "$1=[MASKED]",
	},
	{
		name: "password_field",
		regex: regexp.MustCompile(`(?i)(password|passwd|pwd|pass)\s*[=:]\s*["']?([^"'\s]{4,})["']?`),
		replacement: "$1=[MASKED]",
	},
	{
		name: "database_url",
		regex: regexp.MustCompile(`(?i)(postgres|mysql|mongodb|redis)://[^:]+:[^@]+@[^/]+/\w+`),
		replacement: "$1://[MASKED]@[MASKED]/[MASKED]",
	},
	{
		name: "email_address",
		regex: regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`),
		replacement: "[MASKED_EMAIL]",
	},
	{
		name: "credit_card",
		regex: regexp.MustCompile(`\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12})\b`),
		replacement: "[MASKED_CC]",
	},
	{
		name: "ssn_us",
		regex: regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`),
		replacement: "[MASKED_SSN]",
	},
	{
		name: "ipv4_address",
		regex: regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b`),
		replacement: "[MASKED_IP]",
	},
	{
		name: "ipv6_address",
		regex: regexp.MustCompile(`\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b`),
		replacement: "[MASKED_IPV6]",
	},
	{
		name: "private_key",
		regex: regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----`),
		replacement: "[MASKED_PRIVATE_KEY]",
	},
	{
		name: "ssh_private_key",
		regex: regexp.MustCompile(`-----BEGIN OPENSSH PRIVATE KEY-----[\s\S]*?-----END OPENSSH PRIVATE KEY-----`),
		replacement: "[MASKED_SSH_KEY]",
	},
	{
		name: "certificate",
		regex: regexp.MustCompile(`-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----`),
		replacement: "[MASKED_CERT]",
	},
	{
		name: "generic_secret",
		regex: regexp.MustCompile(`(?i)(secret|token|key)\s*[=:]\s*["']?([A-Za-z0-9_\-+/=]{20,})["']?`),
		replacement: "$1=[MASKED]",
	},
}

func NewMasker(config *models.MaskerConfig, logger *zap.Logger) (*Masker, error) {
	m := &Masker{
		config:      config,
		logger:      logger.Named("masker"),
		patterns:    make([]*compiledPattern, 0, len(defaultPatterns)+len(config.CustomPatterns)),
		customRegex: make([]*regexp.Regexp, 0),
	}

	if !config.Enabled {
		m.logger.Info("Masker disabled")
		return m, nil
	}

	m.patterns = append(m.patterns, defaultPatterns...)

	for _, pattern := range config.CustomPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			m.logger.Warn("Invalid custom pattern, skipping", zap.String("pattern", pattern), zap.Error(err))
			continue
		}
		m.customRegex = append(m.customRegex, re)
		m.patterns = append(m.patterns, &compiledPattern{
			name:        "custom_" + pattern,
			regex:       re,
			replacement: config.MaskReplacement,
		})
	}

	m.logger.Info("Masker initialized",
		zap.Int("builtin_patterns", len(defaultPatterns)),
		zap.Int("custom_patterns", len(m.customRegex)),
		zap.String("replacement", config.MaskReplacement),
	)

	return m, nil
}

func (m *Masker) Mask(line *models.LogLine) *models.LogLine {
	if line == nil {
		return nil
	}
	if !m.config.Enabled || line.Masked {
		return line
	}

	content := line.Content
	originalLen := len(content)
	masked := false

	m.mu.RLock()
	for _, p := range m.patterns {
		if p.regex.MatchString(content) {
			newContent := p.regex.ReplaceAllString(content, p.replacement)
			if newContent != content {
				m.stats.PatternsMatched++
				m.stats.BytesMasked += uint64(len(content) - len(newContent))
				content = newContent
				masked = true
			}
		}
	}
	m.mu.RUnlock()

	if masked {
		m.stats.LinesProcessed++
		line.Content = content
		line.Masked = true

		if m.config.PreserveLength && len(content) != originalLen {
			line.Content = m.padToLength(content, originalLen)
		}
	}

	return line
}

func (m *Masker) MaskString(content string) (string, bool) {
	if !m.config.Enabled {
		return content, false
	}

	originalLen := len(content)
	masked := false

	m.mu.RLock()
	for _, p := range m.patterns {
		if p.regex.MatchString(content) {
			newContent := p.regex.ReplaceAllString(content, p.replacement)
			if newContent != content {
				m.stats.PatternsMatched++
				m.stats.BytesMasked += uint64(len(content) - len(newContent))
				content = newContent
				masked = true
			}
		}
	}
	m.mu.RUnlock()

	if masked && m.config.PreserveLength && len(content) != originalLen {
		content = m.padToLength(content, originalLen)
	}

	return content, masked
}

func (m *Masker) MaskBatch(lines []*models.LogLine) []*models.LogLine {
	if !m.config.Enabled {
		return lines
	}

	for i, line := range lines {
		lines[i] = m.Mask(line)
	}
	return lines
}

func (m *Masker) padToLength(content string, targetLen int) string {
	if len(content) >= targetLen {
		return content[:targetLen]
	}
	return content + strings.Repeat(" ", targetLen-len(content))
}

func (m *Masker) Stats() MaskerStats {
	return m.stats
}

func (m *Masker) AddPattern(name, pattern, replacement string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.patterns = append(m.patterns, &compiledPattern{
		name:        name,
		regex:       re,
		replacement: replacement,
	})

	m.logger.Info("Added custom masking pattern", zap.String("name", name))
	return nil
}

func (m *Masker) RemovePattern(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, p := range m.patterns {
		if p.name == name {
			m.patterns = append(m.patterns[:i], m.patterns[i+1:]...)
			m.logger.Info("Removed masking pattern", zap.String("name", name))
			return true
		}
	}
	return false
}

func (m *Masker) GetPatterns() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, len(m.patterns))
	for i, p := range m.patterns {
		names[i] = p.name
	}
	return names
}