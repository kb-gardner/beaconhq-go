package beaconchi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/kb-gardner/beaconhq-go"
)

type capturedEvent struct {
	Method     string `json:"method"`
	Route      string `json:"route"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	DurationMS int64  `json:"duration_ms"`
	Consumer   string `json:"consumer"`
}

func TestMiddlewareCapturesRouteTemplate(t *testing.T) {
	var (
		mu     sync.Mutex
		events []capturedEvent
		done   = make(chan struct{}, 1)
	)
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []capturedEvent `json:"events"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		mu.Lock()
		events = append(events, body.Events...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case done <- struct{}{}:
		default:
		}
	}))
	defer ingest.Close()

	bc, err := beaconhq.New(beaconhq.Config{APIKey: "k", IngestURL: ingest.URL, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close(context.Background())

	r := chi.NewRouter()
	r.Use(WithConsumerMiddleware(bc))
	r.Get("/users/{id}", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest(http.MethodGet, "/users/123", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest endpoint not called within timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.Method != "GET" {
		t.Errorf("method = %q, want GET", ev.Method)
	}
	if ev.Route != "/users/{id}" {
		t.Errorf("route = %q, want template /users/{id}", ev.Route)
	}
	if ev.Path != "/users/123" {
		t.Errorf("path = %q, want /users/123", ev.Path)
	}
	if ev.Status != http.StatusCreated {
		t.Errorf("status = %d, want 201", ev.Status)
	}
	if ev.DurationMS < 0 {
		t.Errorf("duration_ms = %d, want >= 0", ev.DurationMS)
	}
	if ev.Consumer != "tenant-7" {
		t.Errorf("consumer = %q, want tenant-7", ev.Consumer)
	}
}

// WithConsumerMiddleware is a tiny helper wiring a static consumer for the test.
func WithConsumerMiddleware(bc *beaconhq.Client) func(http.Handler) http.Handler {
	return Middleware(bc, WithConsumer(func(*http.Request) string { return "tenant-7" }))
}
