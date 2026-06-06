// Package beacongin provides Beacon telemetry middleware for the Gin web
// framework (github.com/gin-gonic/gin). It captures the matched route template
// (c.FullPath(), e.g. "/users/:id") rather than the concrete path.
//
//	r := gin.New()
//	bc, _ := beaconhq.New(beaconhq.Config{APIKey: os.Getenv("BEACON_API_KEY")})
//	r.Use(beacongin.Middleware(bc))
package beacongin

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kb-gardner/beaconhq-go"
)

// Option customizes the middleware.
type Option func(*config)

type config struct {
	consumer func(*gin.Context) string
}

// WithConsumer sets a function that derives the consumer identity from the
// request context. Optional.
func WithConsumer(fn func(*gin.Context) string) Option {
	return func(c *config) { c.consumer = fn }
}

// Middleware returns a gin.HandlerFunc that captures one Beacon event per
// request. Fail-open: capture never blocks or panics the request.
func Middleware(client *beaconhq.Client, opts ...Option) gin.HandlerFunc {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	return func(c *gin.Context) {
		if client == nil {
			c.Next()
			return
		}
		start := time.Now()

		c.Next()

		// c.FullPath returns the matched route template, e.g. "/users/:id".
		// Empty for unmatched (404) routes; fall back to the concrete path.
		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}

		var consumer string
		if cfg.consumer != nil {
			consumer = cfg.consumer(c)
		}

		var errMsg string
		if len(c.Errors) > 0 {
			errMsg = c.Errors.String()
		}

		client.Capture(beaconhq.Event{
			Timestamp:  start,
			Method:     c.Request.Method,
			Route:      route,
			Path:       c.Request.URL.Path,
			Status:     c.Writer.Status(),
			DurationMS: time.Since(start).Milliseconds(),
			Consumer:   consumer,
			Error:      errMsg,
		})
	}
}
