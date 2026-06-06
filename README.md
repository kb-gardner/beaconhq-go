# beaconhq-go

Go SDK for [Beacon](https://beacon.skyware.dev) — API monitoring & observability.

`beaconhq-go` captures per-request telemetry (method, route template, status,
latency, consumer) from your Go HTTP services and ships it to Beacon's ingest
API in non-blocking batches. It is **fail-open**: a Beacon outage, a bad key, or
a network blip can never block, slow, or crash the app it is monitoring.

- Asynchronous, batched delivery (timer + size threshold).
- Recovers from panics in every background goroutine.
- Zero third-party dependencies in the core package — framework adapters live in
  separate modules so you only pull the deps for the framework you use.
- Idiomatic middleware for **Gin**, **Echo**, **Chi**, and **Fiber**.

## Install

Core client:

```sh
go get github.com/kb-gardner/beaconhq-go
```

A framework adapter (pulls only that framework's deps):

```sh
go get github.com/kb-gardner/beaconhq-go/gin     # Gin
go get github.com/kb-gardner/beaconhq-go/echo    # Echo
go get github.com/kb-gardner/beaconhq-go/chi     # Chi
go get github.com/kb-gardner/beaconhq-go/fiber   # Fiber
```

Your project key (created in the Beacon dashboard) is read from an env var of
your choice — `BEACON_API_KEY` below.

## Quick start (Gin)

```go
package main

import (
	"context"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/kb-gardner/beaconhq-go"
	beacongin "github.com/kb-gardner/beaconhq-go/gin"
)

func main() {
	bc, err := beaconhq.New(beaconhq.Config{
		APIKey: os.Getenv("BEACON_API_KEY"), // required
		// IngestURL defaults to https://ingest.beacon.skyware.dev/v1/ingest
	})
	if err != nil {
		panic(err)
	}
	defer bc.Close(context.Background()) // flush on shutdown

	r := gin.New()
	r.Use(beacongin.Middleware(bc))

	r.GET("/users/:id", func(c *gin.Context) {
		c.JSON(200, gin.H{"id": c.Param("id")})
	})

	_ = r.Run(":8080")
}
```

The middleware captures the route **template** (`/users/:id`), not the concrete
path (`/users/123`), so requests aggregate correctly in Beacon.

## Other frameworks

### Echo

```go
import (
	"github.com/labstack/echo/v4"
	"github.com/kb-gardner/beaconhq-go"
	beaconecho "github.com/kb-gardner/beaconhq-go/echo"
)

e := echo.New()
e.Use(beaconecho.Middleware(bc))
e.GET("/users/:id", handler) // route captured as "/users/:id"
```

### Chi

```go
import (
	"github.com/go-chi/chi/v5"
	"github.com/kb-gardner/beaconhq-go"
	beaconchi "github.com/kb-gardner/beaconhq-go/chi"
)

r := chi.NewRouter()
r.Use(beaconchi.Middleware(bc))         // register BEFORE routes
r.Get("/users/{id}", handler)           // route captured as "/users/{id}"
```

### Fiber

```go
import (
	"github.com/gofiber/fiber/v2"
	"github.com/kb-gardner/beaconhq-go"
	beaconfiber "github.com/kb-gardner/beaconhq-go/fiber"
)

app := fiber.New()
app.Use(beaconfiber.Middleware(bc))
app.Get("/users/:id", handler)          // route captured as "/users/:id"
```

## Capturing the consumer

Each adapter accepts `WithConsumer` to tag events with the calling identity
(API-key id, user id, tenant…):

```go
r.Use(beacongin.Middleware(bc, beacongin.WithConsumer(func(c *gin.Context) string {
	return c.GetHeader("X-Api-Key-Id")
})))
```

## Manual capture (no framework)

The core client works standalone if you are not using a supported framework:

```go
bc.Capture(beaconhq.Event{
	Method:     "GET",
	Route:      "/users/:id",
	Path:       "/users/123",
	Status:     200,
	DurationMS: 42,
	Consumer:   "acme",
})
```

`Timestamp` defaults to `time.Now()` when left zero.

## Configuration

`beaconhq.Config`:

| Field           | Type                 | Default                                            | Notes |
|-----------------|----------------------|----------------------------------------------------|-------|
| `APIKey`        | `string`             | — (**required**)                                   | Per-project ingest key, sent as `Authorization: Bearer <key>`. |
| `IngestURL`     | `string`             | `https://ingest.beacon.skyware.dev/v1/ingest`      | Override for a self-hosted Beacon. |
| `FlushInterval` | `time.Duration`      | `5s`                                               | Periodic flush cadence. |
| `BatchSize`     | `int`                | `100`                                              | Buffered-event count that triggers an eager flush. |
| `MaxBufferSize` | `int`                | `10000`                                            | Hard cap on the buffer; oldest events drop past this if the network is down. |
| `OnError`       | `func(error)`        | no-op                                              | Optional observability hook; dropped events / failed flushes. Panics inside it are recovered. |
| `HTTPClient`    | `beaconhq.HTTPDoer`  | `*http.Client{Timeout: 10s}`                       | Inject a custom transport (or a fake in tests). |

## Behavior & guarantees

- **Non-blocking**: `Capture` only appends to an in-memory buffer; the network
  call happens on a background goroutine.
- **Batched**: events flush every `FlushInterval` or when the buffer reaches
  `BatchSize`, whichever comes first. Flushes larger than the contract's 1000-event
  cap are automatically split across requests.
- **Fail-open**: 4xx responses (bad key / bad body) are reported and dropped;
  5xx and network errors re-queue the batch (bounded by `MaxBufferSize`). Nothing
  ever propagates a panic or error into your request path.
- **Graceful shutdown**: `Close(ctx)` stops the timer and flushes what remains,
  honoring `ctx` for the final flush deadline.

## Wire format

The SDK speaks the Beacon ingest HTTP contract: `POST /v1/ingest`,
`Authorization: Bearer <key>`, body `{"events":[{ ts, method, route, path,
status, duration_ms, consumer?, error? }]}`. `ts` is RFC3339 with millisecond
precision in UTC.

## License

MIT © Skyware LLC. See [LICENSE](./LICENSE).
