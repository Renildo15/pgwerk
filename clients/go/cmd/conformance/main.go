// The Go client under test for conformance/client (see conformance/README.md).
//
//	URL=… TOKEN=… ./conformance
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pgwerk "github.com/pgwerk/pgwerk/clients/go"
)

func main() {
	opts := []pgwerk.Option{pgwerk.Queues("conformance"), pgwerk.Logger(nil)}
	if strings.HasPrefix(os.Getenv("URL"), "http://") {
		opts = append(opts, pgwerk.Insecure())
	}
	w, err := pgwerk.NewRemoteWorker(os.Getenv("URL"), os.Getenv("TOKEN"), opts...)
	if err != nil {
		log.Fatal(err)
	}

	pgwerk.Handle(w, "conformance.echo", func(_ pgwerk.Context, in map[string]any) (map[string]any, error) {
		return in, nil
	})
	pgwerk.Handle(w, "conformance.sleep", func(ctx pgwerk.Context, in struct{ Seconds float64 }) (any, error) {
		select {
		case <-time.After(time.Duration(in.Seconds * float64(time.Second))):
		case <-ctx.Done():
		}
		return nil, nil
	})
	pgwerk.Handle(w, "conformance.fail", func(_ pgwerk.Context, in struct{ Message string }) (any, error) {
		return nil, errors.New(in.Message)
	})
	pgwerk.Handle(w, "conformance.big", func(_ pgwerk.Context, in struct{ Size int }) (string, error) {
		return strings.Repeat("x", in.Size), nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := w.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
