package beaconhq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ingestBody mirrors the wire envelope so tests can decode what we POST.
type ingestBody struct {
	Events []struct {
		TS         string `json:"ts"`
		Method     string `json:"method"`
		Route      string `json:"route"`
		Path       string `json:"path"`
		Status     int    `json:"status"`
		DurationMS int64  `json:"duration_ms"`
		Consumer   string `json:"consumer"`
		Error      string `json:"error"`
	} `json:"events"`
}

func TestNewRequiresAPIKey(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error when APIKey is empty")
	}
	c, err := New(Config{APIKey: "k"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.ingestURL != DefaultIngestURL {
		t.Fatalf("expected default ingest URL, got %q", c.ingestURL)
	}
	_ = c.Close(context.Background())
}

func TestEventShapeAndAuthHeader(t *testing.T) {
	var (
		mu      sync.Mutex
		gotAuth string
		gotBody ingestBody
		hits    int32
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		mu.Lock()
		defer mu.Unlock()
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted":1}`))
	}))
	defer srv.Close()

	c, err := New(Config{APIKey: "secret-key", IngestURL: srv.URL, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 6, 3, 12, 0, 0, int(500*time.Millisecond), time.UTC)
	c.Capture(Event{
		Timestamp:  ts,
		Method:     "GET",
		Route:      "/users/:id",
		Path:       "/users/123",
		Status:     200,
		DurationMS: 42,
		Consumer:   "acme",
	})

	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	if atomic.LoadInt32(&hits) == 0 {
		t.Fatal("ingest endpoint was never called")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer secret-key" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer secret-key")
	}
	if len(gotBody.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(gotBody.Events))
	}
	ev := gotBody.Events[0]
	if ev.Method != "GET" || ev.Route != "/users/:id" || ev.Path != "/users/123" ||
		ev.Status != 200 || ev.DurationMS != 42 || ev.Consumer != "acme" {
		t.Fatalf("unexpected event payload: %+v", ev)
	}
	if ev.TS != "2026-06-03T12:00:00.500Z" {
		t.Fatalf("ts = %q, want RFC3339 millis UTC", ev.TS)
	}
}

func TestBatchSizeTriggersEagerFlush(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Long interval so only the size threshold can trigger the flush.
	c, err := New(Config{APIKey: "k", IngestURL: srv.URL, BatchSize: 3, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())

	for i := 0; i < 3; i++ {
		c.Capture(Event{Method: "GET", Route: "/x", Status: 200})
	}

	// Wait for the async eager flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&hits) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&hits) == 0 {
		t.Fatal("reaching BatchSize did not trigger an eager flush")
	}
}

func TestFailOpenOnBadURL(t *testing.T) {
	var (
		mu       sync.Mutex
		errCount int
	)
	// Unroutable URL: the HTTP request must fail, but Capture/Close must not.
	c, err := New(Config{
		APIKey:    "k",
		IngestURL: "http://127.0.0.1:1/v1/ingest", // port 1: connection refused
		BatchSize: 1,
		OnError: func(error) {
			mu.Lock()
			errCount++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// None of this should panic or block indefinitely.
	c.Capture(Event{Method: "GET", Route: "/x", Status: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close should fail-open, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if errCount == 0 {
		t.Fatal("expected OnError to be invoked on a failed flush")
	}
}

func TestRecoverDoesNotPanicHost(t *testing.T) {
	// An OnError hook that panics must not propagate.
	c, err := New(Config{
		APIKey:    "k",
		IngestURL: "http://127.0.0.1:1/v1/ingest",
		BatchSize: 1,
		OnError:   func(error) { panic("boom") },
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Capture(Event{Method: "GET", Route: "/x", Status: 500})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx) // must return without panicking
}

func TestStatusRecorder(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := NewStatusRecorder(rec)
	if sr.Status() != http.StatusOK {
		t.Fatalf("default status = %d, want 200", sr.Status())
	}
	sr.WriteHeader(http.StatusTeapot)
	if sr.Status() != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", sr.Status())
	}
	// Second WriteHeader is a no-op.
	sr.WriteHeader(http.StatusOK)
	if sr.Status() != http.StatusTeapot {
		t.Fatalf("status changed after second WriteHeader: %d", sr.Status())
	}
	if _, err := sr.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if rec.Body.String() != "hi" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}
