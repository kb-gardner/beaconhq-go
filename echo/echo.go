// Package beaconecho provides Beacon telemetry middleware for the Echo web
// framework (github.com/labstack/echo). It captures the matched route template
// (c.Path(), e.g. "/users/:id") rather than the concrete path.
//
//	e := echo.New()
//	bc, _ := beaconhq.New(beaconhq.Config{APIKey: os.Getenv("BEACON_API_KEY")})
//	e.Use(beaconecho.Middleware(bc))
package beaconecho

import (
	"time"

	"github.com/kb-gardner/beaconhq-go"
	"github.com/labstack/echo/v4"
)

// Option customizes the middleware.
type Option func(*config)

type config struct {
	consumer func(echo.Context) string
}

// WithConsumer sets a function that derives the consumer identity from the
// request context. Optional.
func WithConsumer(fn func(echo.Context) string) Option {
	return func(c *config) { c.consumer = fn }
}

// Middleware returns an echo.MiddlewareFunc that captures one Beacon event per
// request. Fail-open: capture never blocks or panics the request. The event is
// captured even when the downstream handler returns an error.
func Middleware(client *beaconhq.Client, opts ...Option) echo.MiddlewareFunc {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if client == nil {
				return next(c)
			}
			start := time.Now()

			err := next(c)
			if err != nil {
				// Let Echo's error handler set the response status.
				c.Error(err)
			}

			req := c.Request()
			res := c.Response()

			// c.Path returns the registered route template, e.g. "/users/:id".
			route := c.Path()
			if route == "" {
				route = req.URL.Path
			}

			var consumer string
			if cfg.consumer != nil {
				consumer = cfg.consumer(c)
			}

			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			client.Capture(beaconhq.Event{
				Timestamp:  start,
				Method:     req.Method,
				Route:      route,
				Path:       req.URL.Path,
				Status:     res.Status,
				DurationMS: time.Since(start).Milliseconds(),
				Consumer:   consumer,
				Error:      errMsg,
			})

			// We already invoked c.Error; returning nil prevents Echo from
			// handling the error a second time.
			return nil
		}
	}
}
