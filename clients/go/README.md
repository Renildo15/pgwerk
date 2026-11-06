# pgwerk remote worker for Go

```
go get github.com/pgwerk/pgwerk/clients/go
```

Runs pgwerk jobs from any machine with outbound HTTPS to a pgwerk server
(`werk api --remote-workers`), with no database access. It implements
[remote worker protocol v1](../../docs/reference/remote-protocol.md), passes the client conformance
suite, and uses only the standard library.

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	pgwerk "github.com/pgwerk/pgwerk/clients/go"
)

type RenderIn struct {
	AssetID   string `json:"asset_id"`
	UploadURL string `json:"upload_url"`
}

type RenderOut struct {
	URI string `json:"uri"`
}

func main() {
	w, err := pgwerk.NewRemoteWorker("https://jobs.example.com", os.Getenv("PGWERK_TOKEN"),
		pgwerk.Queues("render"), pgwerk.Concurrency(4))
	if err != nil {
		panic(err)
	}
	pgwerk.Handle(w, "media.tasks.render", func(ctx pgwerk.Context, in RenderIn) (RenderOut, error) {
		ctx.Heartbeat(map[string]string{"stage": "rendering"}) // optional progress
		// ... use ctx for I/O: it is cancelled on abort or lease loss ...
		return RenderOut{URI: "s3://bucket/" + in.AssetID + ".png"}, nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	_ = w.Run(ctx) // on cancel: finishes running jobs, then deregisters
}
```

- The job's `kwargs` decode into the handler's input type, and the output is the job's result
  (JSON). A job enqueued with positional `args` (Python-only) fails with `UnsupportedPayload`.
- `ctx` (a `context.Context`) is cancelled when the job is aborted on the server, or when this worker
  loses the job's lease (its result would be discarded).
- A returned error fails the attempt and follows the job's retries. `pgwerk.NonRetryable(err)` fails
  it for good. A type with `Type() string` sets the reported error type.
- `timeout_secs` is enforced by the client.

## Development

```bash
go test ./...
go build -o bin/conformance ./cmd/conformance
# protocol conformance, from the repo root:
PGWERK_CONFORMANCE_CLIENT=clients/go/bin/conformance uv run pytest conformance/client
```
