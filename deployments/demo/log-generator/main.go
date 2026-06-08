package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	firstNames = []string{"jane", "john", "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "hank",
		"iris", "jack", "kate", "liam", "maya", "nina", "oscar", "paul", "quinn", "ray",
		"sarah", "tom", "uma", "victor", "wendy", "xander", "yara", "zack", "aaron", "bella",
		"carlos", "daisy", "elias", "fiona", "george", "helen", "ivan", "julia", "kevin", "luna"}

	lastNames = []string{"smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis",
		"rodriguez", "martinez", "henderson", "cook", "bailey", "hill", "fox", "cole",
		"hayes", "grant", "stone", "fisher", "tucker", "hamilton", "barnes", "walton",
		"pierce", "fletcher", "ryder", "bishop", "cooper", "morgan"}

	domains = []string{"acme.com", "globex.com", "initech.com", "umbrella.com",
		"stark.com", "wayne.com", "cyberdyne.com", "wonka.com",
		"hooli.com", "piedpiper.com", "massive.com"}

	cities = []struct{ city, state, zip string }{
		{"San Francisco", "CA", "94107"}, {"New York", "NY", "10001"}, {"Austin", "TX", "73301"},
		{"Seattle", "WA", "98101"}, {"Chicago", "IL", "60601"}, {"Boston", "MA", "02101"},
		{"Denver", "CO", "80201"}, {"Portland", "OR", "97201"}, {"Miami", "FL", "33101"},
		{"Atlanta", "GA", "30301"}, {"Phoenix", "AZ", "85001"}, {"Dallas", "TX", "75201"},
	}

	streets = []string{"Main St", "Oak Ave", "Elm St", "Park Blvd", "Broadway", "Lake Dr",
		"Highland Ave", "Cedar Ln", "River Rd", "Maple Ave", "Pine St", "Sunset Blvd"}

	userAgents = []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
		"curl/8.4.0", "Go-http-client/2.0", "python-requests/2.31.0",
		"PostmanRuntime/7.36.0", "okhttp/4.12.0",
	}

	apiPaths = []string{"/api/users", "/api/orders", "/api/products", "/api/inventory",
		"/api/v2/orders", "/api/v2/invoices", "/api/v2/shipments",
		"/api/health", "/api/metrics", "/api/config"}

	userNames = []string{"admin", "root", "app_user", "readonly", "deployer", "monitor", "backup"}
	hostnames = []string{"db-01", "db-02", "db-03", "cache-01", "cache-02", "queue-01",
		"api-01", "api-02", "web-01", "web-02", "worker-01"}
	errorReasons = []string{"connection refused", "timeout", "broken pipe", "no route to host",
		"permission denied", "resource exhausted", "service unavailable", "deadline exceeded",
		"connection reset by peer", "too many open files", "disk full", "out of memory"}

	phoneAreaCodes = []string{"201", "212", "310", "415", "510", "617", "650", "702", "818", "919"}
)

type ServiceType int

const (
	ServiceAPIGateway ServiceType = iota
	ServiceAuth
	ServicePayment
	ServiceDBProxy
	ServiceOrder
	ServiceNotification
	ServiceCount
)

var serviceNames = map[ServiceType]string{
	ServiceAPIGateway:   "api-gateway",
	ServiceAuth:         "auth-service",
	ServicePayment:      "payment-service",
	ServiceDBProxy:      "db-proxy",
	ServiceOrder:        "order-service",
	ServiceNotification: "notification-service",
}

var serviceWeights = []float64{0.30, 0.15, 0.10, 0.15, 0.20, 0.10}

type TrafficController struct {
	baseRate       float64
	burstInterval  float64
	burstDuration  float64
	burstMultiplier float64
	startTime      time.Time
	errorUntil     time.Time
	errorService   ServiceType
}

func NewTrafficController(baseRate, burstInterval, burstDuration, burstMultiplier float64) *TrafficController {
	return &TrafficController{
		baseRate:        baseRate,
		burstInterval:   burstInterval,
		burstDuration:   burstDuration,
		burstMultiplier: burstMultiplier,
		startTime:       time.Now(),
	}
}

func (tc *TrafficController) currentRate() float64 {
	elapsed := time.Since(tc.startTime).Seconds()
	if tc.burstInterval > 0 {
		burstPhase := math.Mod(elapsed, tc.burstInterval)
		if burstPhase < tc.burstDuration {
			return tc.baseRate * tc.burstMultiplier
		}
	}
	return tc.baseRate
}

func (tc *TrafficController) inBurst() bool {
	elapsed := time.Since(tc.startTime).Seconds()
	if tc.burstInterval > 0 {
		burstPhase := math.Mod(elapsed, tc.burstInterval)
		return burstPhase < tc.burstDuration
	}
	return false
}

func (tc *TrafficController) maybeTriggerErrorSpike() *ServiceType {
	if !tc.errorUntil.IsZero() && time.Now().Before(tc.errorUntil) {
		return &tc.errorService
	}
	if !tc.errorUntil.IsZero() && time.Now().After(tc.errorUntil) {
		tc.errorUntil = time.Time{}
		return nil
	}
	if rand.Float64() < 0.008 {
		tc.errorService = ServiceType(rand.Intn(int(ServiceCount)))
		tc.errorUntil = time.Now().Add(time.Duration(15+rand.Intn(25)) * time.Second)
		return &tc.errorService
	}
	return nil
}

func (tc *TrafficController) nextLineDelay() time.Duration {
	rate := tc.currentRate()
	if rate <= 0 {
		return 100 * time.Millisecond
	}
	lambda := 1.0 / rate
	delay := -math.Log(1.0-rand.Float64()) * lambda
	if delay > 1.0 {
		delay = 1.0
	}
	return time.Duration(delay * float64(time.Second))
}

func weightedService() ServiceType {
	r := rand.Float64()
	var cum float64
	for i, w := range serviceWeights {
		cum += w
		if r <= cum {
			return ServiceType(i)
		}
	}
	return ServiceAPIGateway
}

func randomIP() string {
	if rand.Float64() < 0.3 {
		return fmt.Sprintf("10.%d.%d.%d", rand.Intn(256), rand.Intn(256), 1+rand.Intn(254))
	}
	if rand.Float64() < 0.3 {
		return fmt.Sprintf("192.168.%d.%d", rand.Intn(256), 1+rand.Intn(254))
	}
	return fmt.Sprintf("%d.%d.%d.%d", 1+rand.Intn(223), rand.Intn(256), rand.Intn(256), 1+rand.Intn(254))
}

func randomEmail() string {
	n := rand.Intn(len(firstNames))
	ln := rand.Intn(len(lastNames))
	sep := ""
	switch rand.Intn(3) {
	case 0:
		sep = "."
	case 1:
		sep = "_"
	}
	return fmt.Sprintf("%s%s%s@%s", firstNames[n], sep, lastNames[ln], domains[rand.Intn(len(domains))])
}

func randomCreditCard() string {
	prefixes := []string{"4111", "5500", "3400", "3000", "6011", "3714", "4532", "4916"}
	prefix := prefixes[rand.Intn(len(prefixes))]
	parts := []string{prefix}
	for i := 0; i < 3; i++ {
		parts = append(parts, fmt.Sprintf("%04d", rand.Intn(10000)))
	}
	return strings.Join(parts, "-")
}

func randomPhone() string {
	ac := phoneAreaCodes[rand.Intn(len(phoneAreaCodes))]
	return fmt.Sprintf("+1-%s-%03d-%04d", ac, rand.Intn(1000), rand.Intn(10000))
}

func randomSSN() string {
	return fmt.Sprintf("%03d-%02d-%04d", 1+rand.Intn(899), 1+rand.Intn(99), rand.Intn(10000))
}

func randomAddress() string {
	c := cities[rand.Intn(len(cities))]
	return fmt.Sprintf("%d %s, %s, %s %s", 100+rand.Intn(9000), streets[rand.Intn(len(streets))],
		c.city, c.state, c.zip)
}

func randomName() string {
	return fmt.Sprintf("%s %s",
		strings.Title(firstNames[rand.Intn(len(firstNames))]),
		strings.Title(lastNames[rand.Intn(len(lastNames))]))
}

func randomToken() string {
	hdr := randomBase64(24)
	payload := randomBase64(80)
	sig := randomBase64(43)
	return fmt.Sprintf("%s.%s.%s", hdr, payload, sig)
}

func randomBase64(n int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func randomPath() string {
	base := apiPaths[rand.Intn(len(apiPaths))]
	if rand.Float64() < 0.4 {
		return fmt.Sprintf("%s/%d", base, 10000+rand.Intn(90000))
	}
	return base
}

func randomOrderID() string {
	return fmt.Sprintf("ORD-%05d", rand.Intn(100000))
}

func randomTxnID() string {
	return fmt.Sprintf("txn_%s", randomBase64(12))
}

func randomErrno() int {
	return 1000 + rand.Intn(9999)
}

func formatTS(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z07:00")
}

func formatApacheTS(t time.Time) string {
	return t.Format("02/Jan/2006:15:04:05 -0700")
}

func apacheMethod() string {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	return methods[rand.Intn(len(methods))]
}

func apacheStatus(errMode bool) int {
	if errMode {
		status := []int{500, 502, 503, 504}
		return status[rand.Intn(len(status))]
	}
	status := []int{200, 200, 200, 201, 204, 301, 302, 304, 400, 401, 403, 404}
	return status[rand.Intn(len(status))]
}

func logLevel(errMode, weightHigh bool) string {
	if errMode {
		r := rand.Float64()
		switch {
		case r < 0.60:
			return "ERROR"
		case r < 0.75:
			return "WARN"
		case r < 0.90:
			return "INFO"
		default:
			return "DEBUG"
		}
	}
	if weightHigh {
		r := rand.Float64()
		switch {
		case r < 0.05:
			return "ERROR"
		case r < 0.20:
			return "WARN"
		case r < 0.85:
			return "INFO"
		default:
			return "DEBUG"
		}
	}
	r := rand.Float64()
	switch {
	case r < 0.01:
		return "FATAL"
	case r < 0.05:
		return "ERROR"
	case r < 0.20:
		return "WARN"
	case r < 0.80:
		return "INFO"
	default:
		return "DEBUG"
	}
}

var healthServices = []ServiceType{ServiceAPIGateway, ServiceAuth, ServiceDBProxy, ServiceOrder}

func generateLine(service ServiceType, t time.Time, isBurst, errMode bool) string {
	level := logLevel(errMode, isBurst)
	ts := formatTS(t)

	switch service {
	case ServiceAPIGateway:
		ip := randomIP()
		method := apacheMethod()
		path := randomPath()
		status := apacheStatus(errMode)
		bodyBytes := rand.Intn(65536)
		ua := userAgents[rand.Intn(len(userAgents))]
		ats := formatApacheTS(t)
		if rand.Float64() < 0.15 {
			token := randomToken()
			return fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "https://%s%s" "%s" auth=Bearer %s`,
				ip, ats, method, path, status, bodyBytes, domains[rand.Intn(len(domains))], path, ua, token)
		}
		return fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "https://%s" "%s"`,
			ip, ats, method, path, status, bodyBytes, domains[rand.Intn(len(domains))], ua)

	case ServiceAuth:
		email := randomEmail()
		ip := randomIP()
		action := "Login"
		if rand.Float64() < 0.15 {
			action = "Password reset"
		} else if rand.Float64() < 0.10 {
			action = "MFA challenge"
		} else if rand.Float64() < 0.05 {
			action = "Account lockout"
		}
		result := "success"
		if errMode || rand.Float64() < 0.10 {
			result = "failure"
		}
		reqID := randomBase64(16)
		return fmt.Sprintf("%s %s keystone.auth [req-%s] %s: %s from %s - %s", ts, level, reqID, action, email, ip, result)

	case ServicePayment:
		cc := randomCreditCard()
		amount := 5.0 + rand.Float64()*495.0
		txnID := randomTxnID()
		action := "Charge"
		if rand.Float64() < 0.08 {
			action = "Refund"
		}
		result := "approved"
		if errMode {
			result = "declined"
			if rand.Float64() < 0.5 {
				result = "insufficient_funds"
			}
		} else if rand.Float64() < 0.03 {
			result = "declined"
		}
		return fmt.Sprintf("%s %s [payment] %s %s $%.2f - %s (ref:%s)", ts, level, action, cc, amount, result, txnID)

	case ServiceDBProxy:
		email := randomEmail()
		qtypes := []string{"SLOW", "LOCK", "DEADLOCK", "ERROR"}
		qtype := qtypes[rand.Intn(len(qtypes))]
		if errMode || level == "ERROR" || level == "WARN" || isBurst {
			qtype = qtypes[1+rand.Intn(3)]
		}
		dur := 0.1 + rand.Float64()*9.9
		host := fmt.Sprintf("%s.internal", hostnames[rand.Intn(len(hostnames))])
		user := userNames[rand.Intn(len(userNames))]
		errno := randomErrno()
		return fmt.Sprintf(`%s %s [mysql] %s QUERY: SELECT * FROM users WHERE email='%s' (%.2fs) [errno=%d] %s@%s`,
			ts, level, qtype, email, dur, errno, user, host)

	case ServiceOrder:
		orderID := randomOrderID()
		email := randomEmail()
		addr := randomAddress()
		if errMode {
			return fmt.Sprintf("%s %s [order] Order %s FAILED for %s - payment declined", ts, level, orderID, email)
		}
		actions := []string{"created", "shipped", "delivered", "updated", "cancelled"}
		action := actions[rand.Intn(len(actions))]
		return fmt.Sprintf("%s %s [order] Order %s %s by %s, ship to: %s", ts, level, orderID, action, email, addr)

	case ServiceNotification:
		phone := randomPhone()
		ntype := "SMS"
		if rand.Float64() < 0.25 {
			ntype = "Push"
		} else if rand.Float64() < 0.10 {
			ntype = "Email"
		}
		if errMode || rand.Float64() < 0.08 {
			reason := errorReasons[rand.Intn(len(errorReasons))]
			return fmt.Sprintf("%s %s [notification] %s delivery FAILED: %s - %s", ts, level, ntype, phone, reason)
		}
		code := 10000 + rand.Intn(90000)
		return fmt.Sprintf("%s %s [notification] %s sent to %s: \"Your code is %d\"", ts, level, ntype, phone, code)

	default:
		return fmt.Sprintf("%s %s [unknown] unhandled service log", ts, level)
	}
}

func runLogGenerator(config map[string]float64) error {
	logFile := "/var/log/iye/app.log"
	if v := os.Getenv("LOG_FILE"); v != "" {
		logFile = v
	} else {
		if err := os.MkdirAll("/var/log/iye", 0755); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}
	}

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	baseRate := config["base_rate"]
	if baseRate == 0 {
		baseRate = 20
	}
	burstInterval := config["burst_interval"]
	if burstInterval == 0 {
		burstInterval = 45
	}
	burstDuration := config["burst_duration"]
	if burstDuration == 0 {
		burstDuration = 10
	}
	burstMultiplier := config["burst_multiplier"]
	if burstMultiplier == 0 {
		burstMultiplier = 5
	}

	tc := NewTrafficController(baseRate, burstInterval, burstDuration, burstMultiplier)

	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()

	lineCount := 0

	for {
		now := time.Now()

		errSvc := tc.maybeTriggerErrorSpike()
		errMode := errSvc != nil
		isBurst := tc.inBurst()

		var svc ServiceType
		if errMode && errSvc != nil {
			svc = *errSvc
		} else {
			svc = weightedService()
		}

		line := generateLine(svc, now, isBurst, errMode)
		if _, err := fmt.Fprintln(f, line); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		lineCount++

		if lineCount%100 == 0 {
			f.Sync()
		}

		select {
		case <-healthTicker.C:
			for _, hs := range healthServices {
				hLine := generateLine(hs, time.Now(), false, false)
				fmt.Fprintln(f, hLine)
				lineCount++
			}
		default:
		}

		delay := tc.nextLineDelay()
		time.Sleep(delay)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	config := map[string]float64{}

	if v := os.Getenv("LOG_RATE"); v != "" {
		config["base_rate"], _ = strconv.ParseFloat(v, 64)
	} else {
		config["base_rate"] = 20
	}
	if v := os.Getenv("BURST_INTERVAL"); v != "" {
		config["burst_interval"], _ = strconv.ParseFloat(v, 64)
	} else {
		config["burst_interval"] = 45
	}
	if v := os.Getenv("BURST_DURATION"); v != "" {
		config["burst_duration"], _ = strconv.ParseFloat(v, 64)
	} else {
		config["burst_duration"] = 10
	}
	if v := os.Getenv("BURST_MULTIPLIER"); v != "" {
		config["burst_multiplier"], _ = strconv.ParseFloat(v, 64)
	} else {
		config["burst_multiplier"] = 5
	}

	if err := runLogGenerator(config); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
