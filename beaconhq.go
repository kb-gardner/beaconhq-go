// Package beaconhq is the Go SDK for Beacon (https://beacon.skyware.dev), an
// API-monitoring / observability service.
//
// A Client buffers per-request telemetry events and flushes them to the Beacon
// ingest API on an interval or when the buffer fills. It is designed to be
// non-blocking and to NEVER panic into the host application: capture and flush
// failures are recovered and optionally surfaced via the OnError hook, then the
// affected events are dropped or re-queued. This "fail-open" posture means a
// Beacon outage can never take down the app it is monitoring.
//
// The core package has no third-party dependencies. Framework middlewares live
// in their own modules under the gin/, echo/, chi/ and fiber/ subdirectories so
// that importing the core does not pull in every framework's dependencies.
//
//	bc, _ := beaconhq.New(beaconhq.Config{APIKey: os.Getenv("BEACON_API_KEY")})
//	defer bc.Close(context.Background())
//	bc.Capture(beaconhq.Event{Method: "GET", Route: "/users/:id", Status: 200})
package beaconhq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DefaultIngestURL is the hosted Beacon ingest endpoint. With just an APIKey, a
// Client ships to Beacon's managed ingest service — customers only need a
// project key. Set Config.IngestURL to override (e.g. for a self-hosted Beacon).
const DefaultIngestURL = "https://ingest.beacon.skyware.dev/v1/ingest"

const (
	defaultFlushInterval = 5 * time.Second
	defaultBatchSize     = 100
	defaultMaxBufferSize = 10000

	// maxEventsPerRequest mirrors the ingest contract's hard cap of 1000 events
	// per POST. Larger flushes are split across multiple requests.
	maxEventsPerRequest = 1000
)

// Event is a single request-telemetry record. Its JSON shape matches the Beacon
// ingest HTTP contract (POST /v1/ingest).
type Event struct {
	// Timestamp is when the request completed. Serialized as RFC3339 with
	// millisecond precision. Zero values are filled in with time.Now at capture.
	Timestamp time.Time `json:"-"`

	// Method is the HTTP method, e.g. "GET".
	Method string `json:"method"`
	// Route is the route TEMPLATE used for aggregation, e.g. "/users/:id".
	// Always send the template (not the concrete path) so /users/1 and /users/2
	// group together.
	Route string `json:"route"`
	// Path is the concrete request path, e.g. "/users/123".
	Path string `json:"path"`
	// Status is the HTTP status code (100–599).
	Status int `json:"status"`
	// DurationMS is the request duration in milliseconds.
	DurationMS int64 `json:"duration_ms"`
	// Consumer identifies the caller (API key id, user id, …). Optional.
	Consumer string `json:"consumer,omitempty"`
	// Error is an error message if the request failed. Optional.
	Error string `json:"error,omitempty"`
}

// wireEvent is the on-the-wire representation. It adds the ISO-8601 "ts" field
// (the contract's required timestamp) which we render explicitly so the format
// is exactly what the ingest service expects regardless of the caller's
// time.Time location.
type wireEvent struct {
	TS         string `json:"ts"`
	Method     string `json:"method"`
	Route      string `json:"route"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	DurationMS int64  `json:"duration_ms"`
	Consumer   string `json:"consumer,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (e Event) toWire() wireEvent {
	return wireEvent{
		TS:         e.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		Method:     e.Method,
		Route:      e.Route,
		Path:       e.Path,
		Status:     e.Status,
		DurationMS: e.DurationMS,
		Consumer:   e.Consumer,
		Error:      e.Error,
	}
}

// HTTPDoer is the subset of *http.Client the SDK uses. It lets tests inject a
// fake transport.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config configures a Client. Only APIKey is required.
type Config struct {
	// APIKey is the per-project ingest key, sent as "Authorization: Bearer ...".
	// Required.
	APIKey string

	// IngestURL overrides the ingest endpoint. Optional; defaults to
	// DefaultIngestURL.
	IngestURL string

	// FlushInterval is the periodic flush cadence. Optional; default 5s.
	FlushInterval time.Duration
	// BatchSize is the buffered-event count that triggers an eager flush.
	// Optional; default 100.
	BatchSize int
	// MaxBufferSize bounds memory if the network is down; the oldest events are
	// dropped past this. Optional; default 10000.
	MaxBufferSize int

	// OnError is an optional observability hook. It is invoked (best-effort,
	// never on the hot path of an HTTP handler) when an event is dropped or a
	// flush fails. Never required; panics inside it are recovered.
	OnError func(error)

	// HTTPClient injects an HTTP client (tests / custom transports). Optional;
	// defaults to an *http.Client with a 10s timeout.
	HTTPClient HTTPDoer
}

// Client buffers events and flushes them to Beacon asynchronously. It is safe
// for concurrent use by multiple goroutines.
type Client struct {
	apiKey        string
	ingestURL     string
	batchSize     int
	maxBufferSize int
	flushInterval time.Duration
	onError       func(error)
	httpClient    HTTPDoer

	mu     sync.Mutex
	buffer []Event

	// flushing is held for the duration of a flush so concurrent triggers
	// coalesce instead of stampeding the network.
	flushMu sync.Mutex

	stop     chan struct{}
	stopped  chan struct{}
	closeOne sync.Once
}

// New constructs and starts a Client. It returns an error only if APIKey is
// empty; everything else has a safe default.
func New(cfg Config) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("beaconhq: APIKey is required")
	}

	c := &Client{
		apiKey:        cfg.APIKey,
		ingestURL:     orString(cfg.IngestURL, DefaultIngestURL),
		batchSize:     orInt(cfg.BatchSize, defaultBatchSize),
		maxBufferSize: orInt(cfg.MaxBufferSize, defaultMaxBufferSize),
		flushInterval: orDuration(cfg.FlushInterval, defaultFlushInterval),
		onError:       cfg.OnError,
		httpClient:    cfg.HTTPClient,
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	if c.onError == nil {
		c.onError = func(error) {}
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	go c.runLoop()
	return c, nil
}

// Capture enqueues an event. It is non-blocking and never panics: a full buffer
// drops the oldest event (reported via OnError), and reaching BatchSize triggers
// an asynchronous eager flush. A zero Timestamp is set to time.Now().
func (c *Client) Capture(event Event) {
	defer c.recover("capture")

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	c.mu.Lock()
	if len(c.buffer) >= c.maxBufferSize {
		// Drop the oldest to bound memory; report via OnError.
		c.buffer = c.buffer[1:]
		c.reportf("buffer full; dropping oldest event")
	}
	c.buffer = append(c.buffer, event)
	full := len(c.buffer) >= c.batchSize
	c.mu.Unlock()

	if full {
		// Eager flush off the hot path.
		go func() {
			defer c.recover("eager-flush")
			c.flush(context.Background())
		}()
	}
}

// runLoop drives the periodic flush. It exits when the client is closed.
func (c *Client) runLoop() {
	defer close(c.stopped)
	defer c.recover("flush-loop")

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flush(context.Background())
		case <-c.stop:
			return
		}
	}
}

// Flush ships all currently buffered events, blocking until the network call(s)
// complete or ctx is done. Most callers rely on the automatic timer/threshold
// flushing and never call this directly; it is exported mainly for Close and
// tests.
func (c *Client) Flush(ctx context.Context) {
	defer c.recover("flush")
	c.flush(ctx)
}

func (c *Client) flush(ctx context.Context) {
	// Coalesce concurrent flushes: only one runs at a time.
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.buffer
	c.buffer = nil
	c.mu.Unlock()

	// Split into <=maxEventsPerRequest chunks per the ingest contract.
	var failed []Event
	for start := 0; start < len(batch); start += maxEventsPerRequest {
		end := start + maxEventsPerRequest
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[start:end]
		if requeue := c.send(ctx, chunk); requeue {
			failed = append(failed, chunk...)
		}
	}

	if len(failed) > 0 {
		c.requeue(failed)
	}
}

// send POSTs one chunk. It returns true if the chunk should be re-queued
// (network error or 5xx); 4xx (auth/validation) is dropped to avoid an infinite
// retry of a permanently-bad request.
func (c *Client) send(ctx context.Context, chunk []Event) (requeue bool) {
	wire := make([]wireEvent, len(chunk))
	for i, e := range chunk {
		wire[i] = e.toWire()
	}
	body, err := json.Marshal(struct {
		Events []wireEvent `json:"events"`
	}{Events: wire})
	if err != nil {
		c.report(fmt.Errorf("marshal events: %w", err))
		return false // unencodable: dropping is the only sane option
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ingestURL, bytes.NewReader(body))
	if err != nil {
		c.report(fmt.Errorf("build request: %w", err))
		return true
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.report(fmt.Errorf("ingest request failed: %w", err))
		return true // network failure: re-queue (bounded by MaxBufferSize)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false
	}
	c.reportf("ingest responded %d", resp.StatusCode)
	// Re-queue only on server errors; drop on 4xx (bad key / bad body).
	return resp.StatusCode >= 500
}

// requeue prepends failed events back onto the buffer, honoring MaxBufferSize by
// trimming the oldest.
func (c *Client) requeue(events []Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buffer = append(events, c.buffer...)
	if overflow := len(c.buffer) - c.maxBufferSize; overflow > 0 {
		c.buffer = c.buffer[overflow:]
		c.reportf("buffer full after re-queue; dropped %d oldest event(s)", overflow)
	}
}

// Close stops the flush timer and flushes remaining events. It blocks until the
// final flush completes or ctx is done. Safe to call multiple times. Call it on
// graceful shutdown:
//
//	defer bc.Close(context.Background())
func (c *Client) Close(ctx context.Context) error {
	c.closeOne.Do(func() {
		close(c.stop)
		<-c.stopped // wait for the loop goroutine to exit
	})
	// Final flush of whatever is buffered.
	done := make(chan struct{})
	go func() {
		defer c.recover("close-flush")
		defer close(done)
		c.flush(ctx)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recover swallows any panic so the SDK can never crash the host app. It is
// deferred at every goroutine entry point and every exported method that could
// run user-supplied state.
func (c *Client) recover(where string) {
	if r := recover(); r != nil {
		c.report(fmt.Errorf("beaconhq: recovered panic in %s: %v", where, r))
	}
}

func (c *Client) report(err error) {
	if err == nil || c.onError == nil {
		return
	}
	// Guard against a panicking OnError hook.
	defer func() { _ = recover() }()
	c.onError(err)
}

func (c *Client) reportf(format string, args ...any) {
	c.report(fmt.Errorf(format, args...))
}

func orString(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func orDuration(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
