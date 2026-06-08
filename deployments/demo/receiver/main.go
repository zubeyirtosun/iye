package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

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

type Stats struct {
	total     atomic.Uint64
	masked    atomic.Uint64
	errors    atomic.Uint64
	warns     atomic.Uint64
	lastPrint time.Time
}

var stats Stats

const ansiReset = "\033[0m"
const ansiRed = "\033[31m"
const ansiGreen = "\033[32m"
const ansiYellow = "\033[33m"
const ansiBlue = "\033[34m"
const ansiMagenta = "\033[35m"
const ansiCyan = "\033[36m"
const ansiGray = "\033[90m"
const ansiBold = "\033[1m"

func levelColor(level string) string {
	switch strings.ToUpper(level) {
	case "FATAL", "PANIC":
		return ansiRed + ansiBold
	case "ERROR":
		return ansiRed
	case "WARN":
		return ansiYellow
	case "INFO":
		return ansiGreen
	case "DEBUG":
		return ansiGray
	default:
		return ansiCyan
	}
}

func handleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload batchPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, fmt.Sprintf("json error: %v", err), http.StatusBadRequest)
		return
	}

	if payload.Compressed {
		http.Error(w, "compressed payload not supported", http.StatusBadRequest)
		return
	}

	for _, entry := range payload.Entries {
		stats.total.Add(1)

		msg := entry.Message
		if strings.Contains(msg, "MASKED") {
			stats.masked.Add(1)
		}

		lvl := strings.ToUpper(entry.Level)
		if lvl == "" || lvl == "UNKNOWN" {
			lvl = detectLevel(msg)
		}
		switch lvl {
		case "ERROR", "FATAL", "PANIC":
			stats.errors.Add(1)
		case "WARN":
			stats.warns.Add(1)
		}

		color := levelColor(lvl)
		ts := entry.Timestamp
		if len(ts) > 19 {
			ts = ts[:19]
		}
		source := entry.Source
		if source == "" {
			source = "iye"
		}

		fmt.Printf("%s  %s[%5s]%s  %s  %s\n",
			ansiGray+ts+ansiReset,
			color, lvl, ansiReset,
			ansiCyan+source+ansiReset,
			entry.Message)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"received": len(payload.Entries),
		"status":   "ok",
	})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	total := stats.total.Load()
	masked := stats.masked.Load()
	errs := stats.errors.Load()
	warns := stats.warns.Load()

	ratio := 0.0
	if total > 0 {
		ratio = float64(masked) / float64(total) * 100
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "IYE Receiver - Log Output\n")
	fmt.Fprintf(w, "=========================\n\n")
	fmt.Fprintf(w, "Total entries received:  %d\n", total)
	fmt.Fprintf(w, "Entries with [MASKED]:   %d (%.1f%%)\n", masked, ratio)
	fmt.Fprintf(w, "Errors:                  %d\n", errs)
	fmt.Fprintf(w, "Warnings:                %d\n", warns)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "This shows that PII (credit cards, emails, IPs, phones,\n")
	fmt.Fprintf(w, "SSNs, addresses) in the log stream was detected and\n")
	fmt.Fprintf(w, "replaced with [MASKED] by IYE.\n")
}

func detectLevel(msg string) string {
	fields := strings.Fields(msg)
	for _, f := range fields {
		u := strings.ToUpper(f)
		switch u {
		case "FATAL", "PANIC", "ERROR", "WARN", "INFO", "DEBUG":
			return u
		}
	}
	return "UNKNOWN"
}

func printBanner() {
	fmt.Println()
	fmt.Println("  ╔═══════════════════════════════════════════════════╗")
	fmt.Println("  ║         IYE Demo Log Receiver                     ║")
	fmt.Println("  ╠═══════════════════════════════════════════════════╣")
	fmt.Println("  ║  Receiving masked log batches from IYE transport  ║")
	fmt.Println("  ║  Visit /stats for PII masking summary             ║")
	fmt.Println("  ╚═══════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("  Legend:  " + ansiRed + "ERROR" + ansiReset + "   " + ansiYellow + "WARN" + ansiReset + "   " + ansiGreen + "INFO" + ansiReset + "   " + ansiGray + "DEBUG" + ansiReset)
	fmt.Println()
}

func main() {
	printBanner()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/logs", handleBatch)
	mux.HandleFunc("/api/v1/logs", handleBatch)
	mux.HandleFunc("/stats", handleStats)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	port := os.Getenv("RECEIVER_PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		server.Close()
	}()

	log.Printf("listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
