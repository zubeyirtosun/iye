package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func TestMetricsCollector_ProcessLine(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	line := &models.LogLine{
		Timestamp:  time.Now(),
		Source:     "/var/log/app.log",
		Content:    `User login: email=user@example.com`,
		Severity:   models.SeverityInfo,
		Labels:     map[string]string{"app": "auth"},
		Masked:     true,
		Sampled:    true,
	}

	m.ProcessLine(line)

	stats := m.Stats()
	if stats.LinesProcessed != 1 {
		t.Errorf("Expected 1 line processed, got %d", stats.LinesProcessed)
	}
	if stats.BytesProcessed != uint64(len(line.Content)) {
		t.Errorf("Expected %d bytes processed, got %d", len(line.Content), stats.BytesProcessed)
	}
}

func TestMetricsCollector_InferSeverity(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	testCases := []struct {
		input    string
		expected string
	}{
		{`This is a panic situation`, "fatal"},
		{`Fatal error occurred`, "fatal"},
		{`Error: connection failed`, "error"},
		{`Exception in thread`, "error"},
		{`Failed to connect`, "error"},
		{`Warning: high memory usage`, "warn"},
		{`Warn: deprecated API`, "warn"},
		{`Debug: entering function`, "debug"},
		{`Info: startup complete`, "info"},
		{`Normal log message`, "info"},
	}

	for _, tc := range testCases {
		result := m.inferSeverity(tc.input)
		if result != tc.expected {
			t.Errorf("Input: %s\nExpected: %s\nGot: %s", tc.input, tc.expected, result)
		}
	}
}

func TestMetricsCollector_HealthHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	m.healthHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %d", rr.Code)
	}
	if rr.Body.String() != "OK" {
		t.Errorf("Expected body 'OK', got '%s'", rr.Body.String())
	}
}

func TestMetricsCollector_ReadyHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	m.readyHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %d", rr.Code)
	}
	if rr.Body.String() != "Ready" {
		t.Errorf("Expected body 'Ready', got '%s'", rr.Body.String())
	}
}

func TestMetricsCollector_AddCustomPattern(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	err = m.AddCustomPattern("user_id", `user_id=(\w+)`)
	if err != nil {
		t.Fatal(err)
	}

	line := &models.LogLine{
		Timestamp:  time.Now(),
		Source:     "/var/log/app.log",
		Content:    `User login: user_id=john123`,
		Severity:   models.SeverityInfo,
	}

	m.ProcessLine(line)

	// Check that pattern was matched (we can't easily check the counter value without a registry)
	_, exists := m.customPatterns["user_id"]
	if !exists {
		t.Error("Custom pattern not added")
	}
}

func TestMetricsCollector_RemoveCustomPattern(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.AddCustomPattern("test_pattern", `test=(\d+)`)
	if !m.RemoveCustomPattern("test_pattern") {
		t.Error("Failed to remove custom pattern")
	}
	if m.RemoveCustomPattern("nonexistent") {
		t.Error("Should not be able to remove nonexistent pattern")
	}
}

func TestMetricsCollector_RecordAnomalyEvent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.RecordAnomalyEvent("error_rate", "/var/log/app.log")
	// We can't easily test the counter value without accessing the registry directly
}

func TestMetricsCollector_SetSamplingMode(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.SetSamplingMode("/var/log/app.log", true)
	// Check that internal state is true
	if !m.samplingMode["/var/log/app.log"] {
		t.Error("Expected sampling mode to be true")
	}

	m.SetSamplingMode("/var/log/app.log", false)
	// Check that internal state is false
	if m.samplingMode["/var/log/app.log"] {
		t.Error("Expected sampling mode to be false")
	}
}

func TestMetricsCollector_UpdateBufferSize(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.UpdateBufferSize("main_buffer", 1024)
	// Can't easily test gauge value without registry access
}

func TestMetricsCollector_RecordBufferDropped(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.RecordBufferDropped("main_buffer", 5)
	// Can't easily test counter value without registry access
}

func TestMetricsCollector_ExtractFields(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	content := `user_id=12345 action=login status=success`
	fields := m.ExtractFields(content)

	if fields["user_id"] != "12345" {
		t.Errorf("Expected user_id=12345, got %s", fields["user_id"])
	}
	if fields["action"] != "login" {
		t.Errorf("Expected action=login, got %s", fields["action"])
	}
	if fields["status"] != "success" {
		t.Errorf("Expected status=success, got %s", fields["status"])
	}
}

func TestMetricsCollector_ExtractMetricsFromJSON(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	content := `{"response_time": 0.123, "status_code": 200, "count": 42}`
	metrics := m.ExtractMetricsFromJSON(content)

	if metrics["response_time"] != 0.123 {
		t.Errorf("Expected response_time=0.123, got %v", metrics["response_time"])
	}
	if metrics["status_code"] != 200 {
		t.Errorf("Expected status_code=200, got %v", metrics["status_code"])
	}
	if metrics["count"] != 42 {
		t.Errorf("Expected count=42, got %v", metrics["count"])
	}
}

func TestMetricsCollector_DropLine(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled:       true,
		ListenAddress: ":0",
		MetricsPath:   "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.DropLine("/var/log/app.log")

	stats := m.Stats()
	if stats.LinesDropped != 1 {
		t.Errorf("Expected 1 dropped line, got %d", stats.LinesDropped)
	}

	m.DropLine("/var/log/app.log")
	stats = m.Stats()
	if stats.LinesDropped != 2 {
		t.Errorf("Expected 2 dropped lines, got %d", stats.LinesDropped)
	}
}

func TestMetricsCollector_ParseLogLevel(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled:       true,
		ListenAddress: ":0",
		MetricsPath:   "/metrics",
	}
	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	tests := []struct {
		input    string
		expected models.SeverityLevel
	}{
		{"panic: nil pointer", models.SeverityPanic},
		{"Fatal error", models.SeverityFatal},
		{"Error: connection refused", models.SeverityError},
		{"Exception in thread", models.SeverityError},
		{"Warning: memory high", models.SeverityWarn},
		{"warn: deprecated", models.SeverityWarn},
		{"Debug: entering", models.SeverityDebug},
		{"Info: server started", models.SeverityInfo},
		{"normal message", models.SeverityInfo},
	}
	for _, tt := range tests {
		result := m.ParseLogLevel(tt.input)
		if result != tt.expected {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestMetricsCollector_SetSourceLabels(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled:       true,
		ListenAddress: ":0",
		MetricsPath:   "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.SetSourceLabels("/var/log/app.log", map[string]string{"app": "nginx", "env": "prod"})
	// No explicit return value, but it shouldn't panic
}

func BenchmarkMetricsCollector_ProcessLine(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	config := &models.MetricsConfig{
		Enabled: true,
		ListenAddress: ":0",
		MetricsPath: "/metrics",
	}

	m, err := NewMetricsCollector(config, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Stop()

	line := &models.LogLine{
		Timestamp:  time.Now(),
		Source:     "/var/log/app.log",
		Content:    `User login: email=user@example.com action=login status=success`,
		Severity:   models.SeverityInfo,
		Labels:     map[string]string{"app": "auth"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ProcessLine(line)
	}
}