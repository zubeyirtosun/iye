package masker

import (
	"testing"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func TestMasker_AWSKeys(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    `aws_access_key_id=AKIAIOSFODNN7EXAMPLE`,
			expected: `aws_access_key_id=[MASKED]`,
		},
		{
			input:    `AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`,
			expected: `AWS_SECRET_ACCESS_KEY=[MASKED]`,
		},
		{
			input:    `AKIAIOSFODNN7EXAMPLE`,
			expected: `[MASKED_AWS_KEY]`,
		},
	}

	for _, tc := range testCases {
		result, masked := m.MaskString(tc.input)
		if result != tc.expected {
			t.Errorf("Input: %s\nExpected: %s\nGot: %s\nMasked: %v", tc.input, tc.expected, result, masked)
		}
		if !masked {
			t.Errorf("Expected masked=true for input: %s", tc.input)
		}
	}
}

func TestMasker_JWTToken(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c`
	result, masked := m.MaskString(input)

	if result != `Authorization: Bearer [MASKED_JWT]` {
		t.Errorf("Expected JWT masked, got: %s", result)
	}
	if !masked {
		t.Error("Expected masked=true")
	}
}

func TestMasker_Email(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `User email: john.doe@example.com and jane@test.org`
	result, masked := m.MaskString(input)

	expected := `User email: [MASKED_EMAIL] and [MASKED_EMAIL]`
	if result != expected {
		t.Errorf("Expected: %s\nGot: %s", expected, result)
	}
	if !masked {
		t.Error("Expected masked=true")
	}
}

func TestMasker_CreditCard(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    `Card: 4111111111111111`,
			expected: `Card: [MASKED_CC]`,
		},
		{
			input:    `Card: 5555555555554444`,
			expected: `Card: [MASKED_CC]`,
		},
		{
			input:    `Card: 378282246310005`,
			expected: `Card: [MASKED_CC]`,
		},
	}

	for _, tc := range testCases {
		result, masked := m.MaskString(tc.input)
		if result != tc.expected {
			t.Errorf("Input: %s\nExpected: %s\nGot: %s", tc.input, tc.expected, result)
		}
		if !masked {
			t.Errorf("Expected masked=true for: %s", tc.input)
		}
	}
}

func TestMasker_SSN(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `SSN: 123-45-6789`
	result, masked := m.MaskString(input)

	if result != `SSN: [MASKED_SSN]` {
		t.Errorf("Expected SSN masked, got: %s", result)
	}
	if !masked {
		t.Error("Expected masked=true")
	}
}

func TestMasker_IPAddresses(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    `Client IP: 192.168.1.100`,
			expected: `Client IP: [MASKED_IP]`,
		},
		{
			input:    `IPv6: 2001:0db8:85a3:0000:0000:8a2e:0370:7334`,
			expected: `IPv6: [MASKED_IPV6]`,
		},
	}

	for _, tc := range testCases {
		result, masked := m.MaskString(tc.input)
		if result != tc.expected {
			t.Errorf("Input: %s\nExpected: %s\nGot: %s", tc.input, tc.expected, result)
		}
		if !masked {
			t.Errorf("Expected masked=true for: %s", tc.input)
		}
	}
}

func TestMasker_PrivateKey(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQD...
-----END PRIVATE KEY-----`
	result, masked := m.MaskString(input)

	if result != `[MASKED_PRIVATE_KEY]` {
		t.Errorf("Expected private key masked, got: %s", result)
	}
	if !masked {
		t.Error("Expected masked=true")
	}
}

func TestMasker_DatabaseURL(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    `postgres://user:pass@localhost:5432/db`,
			expected: `postgres://[MASKED]@[MASKED]/[MASKED]`,
		},
		{
			input:    `mysql://root:secretpassword@db.example.com/mydb`,
			expected: `mysql://[MASKED]@[MASKED]/[MASKED]`,
		},
	}

	for _, tc := range testCases {
		result, masked := m.MaskString(tc.input)
		if result != tc.expected {
			t.Errorf("Input: %s\nExpected: %s\nGot: %s", tc.input, tc.expected, result)
		}
		if !masked {
			t.Errorf("Expected masked=true for: %s", tc.input)
		}
	}
}

func TestMasker_Disabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled: false,
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `password=secret123`
	result, masked := m.MaskString(input)

	if result != input {
		t.Errorf("Expected no change when disabled, got: %s", result)
	}
	if masked {
		t.Error("Expected masked=false when disabled")
	}
}

func TestMasker_CustomPattern(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[REDACTED]",
		CustomPatterns:  []string{`my-secret-\w+`},
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `API key: my-secret-abc123`
	result, masked := m.MaskString(input)

	if result != `API key: [REDACTED]` {
		t.Errorf("Expected custom pattern masked, got: %s", result)
	}
	if !masked {
		t.Error("Expected masked=true")
	}
}

func TestMasker_LogLine(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	line := &models.LogLine{
		Timestamp:  time.Now(),
		Source:     "/var/log/app.log",
		Content:    `User login: email=user@example.com password=secret123`,
		Severity:   models.SeverityInfo,
		Labels:     map[string]string{"app": "auth"},
	}
	originalContent := line.Content

	maskedLine := m.Mask(line)

	if !maskedLine.Masked {
		t.Error("Expected line to be masked")
	}
	if maskedLine.Content == originalContent {
		t.Error("Expected content to be modified")
	}
	if maskedLine.Labels == nil {
		t.Error("Labels should be preserved")
	}
}

func TestMasker_Batch(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	lines := []*models.LogLine{
		{Content: `password=secret1`},
		{Content: `email=test@example.com`},
		{Content: `normal log line`},
	}

	masked := m.MaskBatch(lines)

	if !masked[0].Masked {
		t.Error("First line should be masked")
	}
	if !masked[1].Masked {
		t.Error("Second line should be masked")
	}
	if masked[2].Masked {
		t.Error("Third line should not be masked")
	}
}

func TestMasker_PreserveLength(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:        true,
		MaskReplacement: "[X]",
		PreserveLength:  true,
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	input := `Key: AKIAIOSFODNN7EXAMPLE`
	result, masked := m.MaskString(input)

	// With preserve-length, the replacement should match original length
	if !masked {
		t.Error("Expected masked=true")
	}
	if len(result) != len(input) {
		t.Errorf("Expected preserve-length: input len=%d, output len=%d", len(input), len(result))
	}
}

func TestMasker_Stats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	stats := m.Stats()
	if stats.PatternsMatched != 0 {
		t.Errorf("Expected 0 patterns matched, got %d", stats.PatternsMatched)
	}
}

func TestMasker_AddRemovePattern(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Add custom pattern
	err = m.AddPattern("api-key", `my-api-key-\w+`, "[API_KEY]")
	if err != nil {
		t.Fatal(err)
	}

	input := `key: my-api-key-abc123`
	result, masked := m.MaskString(input)
	if !masked {
		t.Error("Expected masked=true with custom pattern")
	}
	if result != `key: [API_KEY]` {
		t.Errorf("Expected custom pattern masked, got: %s", result)
	}

	// Get patterns (returns names)
	patterns := m.GetPatterns()
	found := false
	for _, n := range patterns {
		if n == "api-key" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Custom pattern 'api-key' not found in GetPatterns()")
	}

	// Remove pattern by name
	removed := m.RemovePattern("api-key")
	if !removed {
		t.Error("Expected RemovePattern to return true")
	}

	// Verify it's gone
	result2, masked2 := m.MaskString(input)
	if masked2 {
		t.Error("Expected masked=false after pattern removed")
	}
	if result2 != input {
		t.Error("Expected no change after pattern removed")
	}
}

func TestMasker_RemoveNonExistentPattern(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled: true,
	}
	m, err := NewMasker(config, logger)
	if err != nil {
		t.Fatal(err)
	}

	if m.RemovePattern("nonexistent") {
		t.Error("Expected false when removing non-existent pattern")
	}
}

func BenchmarkMasker(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	config := &models.MaskerConfig{
		Enabled:         true,
		MaskReplacement: "[MASKED]",
	}

	m, err := NewMasker(config, logger)
	if err != nil {
		b.Fatal(err)
	}

	input := `User login: email=user@example.com password=secret123 api_key=AKIAIOSFODNN7EXAMPLE jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.MaskString(input)
	}
}