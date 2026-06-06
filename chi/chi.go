// Package beaconchi provides Beacon telemetry middleware for the chi router
// (github.com/go-chi/chi). It captures the chi route PATTERN (template) rather
// than the concrete request path, so /users/{id} aggregates correctly.
//
//	r := chi.NewRouter()
//	bc, _ := beaconhq.New(beaconhq.Config{APIKey: os.Getenv("BEACON_API_KEY")})
//	r.Use(beaconchi.Middleware(bc))
//
// Middleware must be registered with r.Use BEFORE the routes are mounted; the
// route pattern is only available from the chi RouteContext after the request
// has been routed, which is why the template is read AFTER next.ServeHTTP.
package beaconchi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/kb-gardner/beaconhq-go"
)

// Option customizes the middleware.
type Option func(*config)

type config struct {
	consumer func(*http.Request) string
}

// WithConsumer sets a function that derives the consumer identity (e.g. an API
// key id or user id) from the request. Optional.
func WithConsumer(fn func(*http.Request) string) Option {
	return func(c *config) { c.consumer = fn }
}

// Middleware returns a chi-compatible func(http.Handler) http.Handler that
// captures one Beacon event per request. It is fail-open: capture never blocks
// or panics the request.
func Middleware(client *beaconhq.Client, opts ...Option) func(http.Handler) http.Handler {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if client == nil {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			rec := beaconhq.NewStatusRecorder(w)

			next.ServeHTTP(rec, r)

			// chi's RouteContext is populated during routing; read the matched
			// pattern now. RoutePattern joins nested mount patterns.
			route := r.URL.Path
			if rctx := chi.RouteContext(r.Context()); rctx != nil {
				if p := rctx.RoutePattern(); p != "" {
					route = p
				}
			}

			var consumer string
			if cfg.consumer != nil {
				consumer = cfg.consumer(r)
			}

			client.Capture(beaconhq.Event{
				Timestamp:  start,
				Method:     r.Method,
				Route:      strings.TrimSuffix(route, "/*"),
				Path:       r.URL.Path,
				Status:     rec.Status(),
				DurationMS: time.Since(start).Milliseconds(),
				Consumer:   consumer,
			})
		})
	}
}
