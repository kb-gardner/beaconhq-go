// Package beaconfiber provides Beacon telemetry middleware for the Fiber web
// framework (github.com/gofiber/fiber). It captures the matched route template
// (c.Route().Path, e.g. "/users/:id") rather than the concrete path.
//
//	app := fiber.New()
//	bc, _ := beaconhq.New(beaconhq.Config{APIKey: os.Getenv("BEACON_API_KEY")})
//	app.Use(beaconfiber.Middleware(bc))
package beaconfiber

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kb-gardner/beaconhq-go"
)

// Option customizes the middleware.
type Option func(*config)

type config struct {
	consumer func(*fiber.Ctx) string
}

// WithConsumer sets a function that derives the consumer identity from the
// request context. Optional.
func WithConsumer(fn func(*fiber.Ctx) string) Option {
	return func(c *config) { c.consumer = fn }
}

// Middleware returns a fiber.Handler that captures one Beacon event per request.
// Fail-open: capture never blocks or panics the request.
func Middleware(client *beaconhq.Client, opts ...Option) fiber.Handler {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	return func(c *fiber.Ctx) error {
		if client == nil {
			return c.Next()
		}
		start := time.Now()

		chainErr := c.Next()

		// c.Route().Path is the registered route template, e.g. "/users/:id".
		route := c.Path()
		if rt := c.Route(); rt != nil && rt.Path != "" {
			route = rt.Path
		}

		var consumer string
		if cfg.consumer != nil {
			consumer = cfg.consumer(c)
		}

		var errMsg string
		if chainErr != nil {
			errMsg = chainErr.Error()
		}

		// Status: after c.Next, the response status reflects any error handled
		// by Fiber's error handler.
		client.Capture(beaconhq.Event{
			Timestamp:  start,
			Method:     c.Method(),
			Route:      route,
			Path:       c.Path(),
			Status:     c.Response().StatusCode(),
			DurationMS: time.Since(start).Milliseconds(),
			Consumer:   consumer,
			Error:      errMsg,
		})

		return chainErr
	}
}
