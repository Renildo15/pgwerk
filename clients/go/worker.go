// Package pgwerk is a pgwerk remote worker for protocol v1
// (docs/reference/remote-protocol.md).
//
// It runs pgwerk jobs from any machine with outbound HTTPS to a pgwerk server: register
// with an enrollment token, long-poll claims, renew leases, report results. Retries,
// dependencies, callbacks and scheduling stay on the server. Standard library only.
//
//	w, err := pgwerk.NewRemoteWorker(url, token, pgwerk.Queues("render"), pgwerk.Concurrency(4))
//	pgwerk.Handle(w, "media.tasks.render", func(ctx pgwerk.Context, in RenderIn) (RenderOut, error) { ... })
//	err = w.Run(ctx)
package pgwerk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Protocol is the remote worker protocol version this client speaks.
	Protocol = "1"
	// ClientVersion is reported to the server at registration.
	ClientVersion = "pgwerk-go/0.1.0"
)

// Job is the job as the server sent it.
type Job struct {
	ID             string
	Function       string
	Queue          string
	Kwargs         json.RawMessage
	Args           []json.RawMessage
	Attempts       int
	MaxAttempts    int
	LostCount      int
	TimeoutSecs    *int
	Meta           json.RawMessage
	LeaseID        string
	LeaseTTL       int
	HeartbeatEvery int
}

// Context is passed to handlers. Done() closes when the server aborts the job or this
// worker loses the job's lease (its result would be discarded).
type Context interface {
	context.Context
	Job() Job
	// Heartbeat records progress (any JSON value), sent with the next lease heartbeat.
	Heartbeat(progress any)
}

// ProtocolError is a protocol error answer ({"error": {"code", "message"}}).
type ProtocolError struct {
	Status  int
	Code    string
	Message string
}

func (e *ProtocolError) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message) }

// Retryable reports whether the same request is worth retrying (429 or a server error).
func (e *ProtocolError) Retryable() bool { return e.Status == 429 || e.Status >= 500 }

type nonRetryable struct{ err error }

func (n nonRetryable) Error() string { return n.err.Error() }
func (n nonRetryable) Unwrap() error { return n.err }

// NonRetryable wraps err so the job fails for good, even with retries left.
func NonRetryable(err error) error { return nonRetryable{err} }

// Option configures a RemoteWorker.
type Option func(*RemoteWorker)

// Queues sets the queues to consume (required; within the enrollment token's scope).
func Queues(q ...string) Option { return func(w *RemoteWorker) { w.queues = q } }

// Concurrency sets how many jobs run at once (default 1); each slot runs its own claim loop.
func Concurrency(n int) Option { return func(w *RemoteWorker) { w.concurrency = n } }

// Name sets the worker's display name (default hostname.pid).
func Name(n string) Option { return func(w *RemoteWorker) { w.name = n } }

// Labels sets free-form metadata stored with the worker.
func Labels(l map[string]any) Option { return func(w *RemoteWorker) { w.labels = l } }

// Insecure allows a plain http:// server URL (local testing only).
func Insecure() Option { return func(w *RemoteWorker) { w.insecure = true } }

// HTTPClient overrides the HTTP client (tests, proxies).
func HTTPClient(c *http.Client) Option { return func(w *RemoteWorker) { w.http = c } }

// Logger overrides where the worker logs (default: the standard logger). Nil silences it.
func Logger(l *log.Logger) Option { return func(w *RemoteWorker) { w.logger = l } }

type handlerFunc func(ctx Context, kwargs json.RawMessage) (any, error)

// RemoteWorker runs jobs from a pgwerk server.
type RemoteWorker struct {
	base        string
	enrollment  string
	queues      []string
	concurrency int
	name        string
	labels      map[string]any
	insecure    bool
	http        *http.Client
	logger      *log.Logger
	handlers    map[string]handlerFunc

	workerID    string
	workerToken string
	leaseTTL    int
	claimWait   int
	draining    atomic.Bool
	revoked     atomic.Bool
	stop        context.CancelFunc
}

// NewRemoteWorker creates a worker for the server at url with an enrollment token.
func NewRemoteWorker(url, token string, opts ...Option) (*RemoteWorker, error) {
	host, _ := os.Hostname()
	w := &RemoteWorker{
		base:        strings.TrimRight(url, "/") + "/remote/v1",
		enrollment:  token,
		concurrency: 1,
		name:        fmt.Sprintf("%s.%d", host, os.Getpid()),
		labels:      map[string]any{},
		http:        &http.Client{},
		logger:      log.Default(),
		handlers:    map[string]handlerFunc{},
		leaseTTL:    60,
		claimWait:   30,
	}
	for _, o := range opts {
		o(w)
	}
	if strings.HasPrefix(url, "http://") && !w.insecure {
		return nil, errors.New("pgwerk: refusing a plain http:// URL; use https://, or Insecure() for local testing")
	}
	if len(w.queues) == 0 {
		return nil, errors.New("pgwerk: Queues(...) is required")
	}
	return w, nil
}

// Handle registers fn for jobs enqueued as name. The job's kwargs decode into In, and
// the returned Out is the job's result (JSON).
func Handle[In, Out any](w *RemoteWorker, name string, fn func(ctx Context, in In) (Out, error)) {
	w.handlers[name] = func(ctx Context, kwargs json.RawMessage) (any, error) {
		var in In
		if len(kwargs) > 0 {
			if err := json.Unmarshal(kwargs, &in); err != nil {
				return nil, NonRetryable(fmt.Errorf("decoding kwargs: %w", err))
			}
		}
		return fn(ctx, in)
	}
}

// WorkerID is set once registered.
func (w *RemoteWorker) WorkerID() string { return w.workerID }

// Revoked reports whether the server stopped accepting this worker's token.
func (w *RemoteWorker) Revoked() bool { return w.revoked.Load() }

// Run registers, then processes jobs until ctx is cancelled (or the worker is revoked or
// told to drain). Running jobs finish first; then it deregisters.
func (w *RemoteWorker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("pgwerk: register at least one handler with Handle")
	}
	if err := w.register(ctx); err != nil {
		return err
	}
	claimCtx, stop := context.WithCancel(ctx)
	w.stop = stop
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.slot(claimCtx)
		}()
	}
	wg.Wait()
	if !w.revoked.Load() {
		dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, _, err := w.call(dctx, "/deregister", w.workerToken, map[string]any{}); err != nil {
			w.logf("deregister failed: %v", err)
		}
	}
	return nil
}

func (w *RemoteWorker) logf(format string, args ...any) {
	if w.logger != nil {
		w.logger.Printf("pgwerk: "+format, args...)
	}
}

// --- protocol calls ----------------------------------------------------------

type transportError struct{ err error }

func (t transportError) Error() string { return t.err.Error() }

func (w *RemoteWorker) call(ctx context.Context, path, token string, body any) (int, []byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.base+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Pgwerk-Protocol", Protocol)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := w.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, transportError{err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var e struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.Error.Code == "" {
			e.Error.Code = "http_error"
		}
		return resp.StatusCode, nil, &ProtocolError{resp.StatusCode, e.Error.Code, e.Error.Message}
	}
	return resp.StatusCode, raw, nil
}

func (w *RemoteWorker) register(ctx context.Context) error {
	functions := make([]map[string]string, 0, len(w.handlers))
	for name := range w.handlers {
		functions = append(functions, map[string]string{"name": name})
	}
	body := map[string]any{
		"name": w.name, "queues": w.queues, "functions": functions, "labels": w.labels,
		"client": map[string]string{"lang": "go", "version": ClientVersion},
	}
	delay := time.Second
	for {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, raw, err := w.call(cctx, "/register", w.enrollment, body)
		cancel()
		if err == nil {
			var r struct {
				WorkerID    string `json:"worker_id"`
				WorkerToken string `json:"worker_token"`
				LeaseTTL    int    `json:"lease_ttl"`
				ClaimWait   int    `json:"claim_wait"`
			}
			if err := json.Unmarshal(raw, &r); err != nil {
				return err
			}
			w.workerID, w.workerToken = r.WorkerID, r.WorkerToken
			if r.LeaseTTL > 0 {
				w.leaseTTL = r.LeaseTTL
			}
			if r.ClaimWait > 0 {
				w.claimWait = r.ClaimWait
			}
			return nil
		}
		var pe *ProtocolError
		if errors.As(err, &pe) && !pe.Retryable() {
			return err
		}
		w.logf("register failed (%v), retrying in %s", err, delay)
		if !sleep(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// --- claim loop ----------------------------------------------------------------

func (w *RemoteWorker) slot(ctx context.Context) {
	claimID := newUUID()
	backoff := time.Second
	for ctx.Err() == nil && !w.draining.Load() && !w.revoked.Load() {
		cctx, cancel := context.WithTimeout(ctx, time.Duration(w.claimWait+10)*time.Second)
		status, raw, err := w.call(cctx, "/claim", w.workerToken, map[string]any{"claim_id": claimID, "wait": w.claimWait})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var pe *ProtocolError
			if errors.As(err, &pe) && pe.Code == "draining" {
				w.logf("worker is draining: finishing held jobs")
				w.draining.Store(true)
				w.stop()
				return
			}
			if errors.As(err, &pe) && pe.Status == 401 {
				w.revoke()
				return
			}
			// Same claim_id on retry: a job already leased to us comes back (spec §5.2).
			w.logf("claim failed (%v), retrying", err)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		claimID = newUUID()
		if status == http.StatusOK {
			job, err := decodeClaim(raw)
			if err != nil {
				w.logf("bad claim response: %v", err)
				continue
			}
			w.runJob(job) // not cancelled by ctx: a running job finishes on shutdown
		}
	}
}

func (w *RemoteWorker) revoke() {
	if !w.revoked.Swap(true) {
		w.logf("worker token no longer valid (revoked or expired); stopping")
	}
	if w.stop != nil {
		w.stop()
	}
}

// --- one job -------------------------------------------------------------------

type jobContext struct {
	context.Context
	job      Job
	mu       sync.Mutex
	progress any
}

func (c *jobContext) Job() Job { return c.job }

func (c *jobContext) Heartbeat(progress any) {
	c.mu.Lock()
	c.progress = progress
	c.mu.Unlock()
}

func (c *jobContext) currentProgress() any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.progress
}

const (
	stateRunning int32 = iota
	stateAborted
	stateLost
)

type outcome struct {
	value any
	err   error
}

func (w *RemoteWorker) runJob(job Job) {
	if len(job.Args) > 0 {
		// kwargs is the cross-language contract; positional args are Python-only (spec §7).
		w.report(job, "/fail", failBody("UnsupportedPayload", "positional args", false))
		return
	}
	handler, ok := w.handlers[job.Function]
	if !ok {
		w.report(job, "/fail", failBody("UnknownFunction", job.Function, false))
		return
	}

	jctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &jobContext{Context: jctx, job: job}
	var state atomic.Int32

	beatCtx, stopBeat := context.WithCancel(context.Background())
	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		ticker := time.NewTicker(time.Duration(max(job.HeartbeatEvery, 1)) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-beatCtx.Done():
				return
			case <-ticker.C:
			}
			hctx, hcancel := context.WithTimeout(beatCtx, 30*time.Second)
			_, raw, err := w.call(hctx, "/leases/"+job.LeaseID+"/heartbeat", w.workerToken,
				map[string]any{"progress": ctx.currentProgress()})
			hcancel()
			if err != nil {
				var pe *ProtocolError
				if errors.As(err, &pe) && (pe.Code == "lease_lost" || pe.Status == 401) {
					state.CompareAndSwap(stateRunning, stateLost)
					cancel()
					if pe.Status == 401 {
						w.revoke()
					}
					return
				}
				if beatCtx.Err() == nil {
					w.logf("heartbeat failed for %s: %v", job.ID, err)
				}
				continue
			}
			var hb struct {
				Abort bool `json:"abort"`
			}
			if json.Unmarshal(raw, &hb) == nil && hb.Abort && state.CompareAndSwap(stateRunning, stateAborted) {
				cancel()
			}
		}
	}()
	finish := func(path string, body map[string]any) {
		stopBeat() // once the report lands the lease is gone; a late heartbeat would get lease_lost
		<-beatDone
		w.report(job, path, body)
	}
	defer func() { stopBeat(); <-beatDone }()

	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- outcome{err: fmt.Errorf("panic: %v", r)}
			}
		}()
		v, err := handler(ctx, job.Kwargs)
		done <- outcome{v, err}
	}()

	var timeout <-chan time.Time
	if job.TimeoutSecs != nil && *job.TimeoutSecs > 0 {
		t := time.NewTimer(time.Duration(*job.TimeoutSecs) * time.Second)
		defer t.Stop()
		timeout = t.C
	}

	select {
	case <-jctx.Done():
		switch state.Load() {
		case stateAborted:
			finish("/fail", map[string]any{"aborted": true})
		case stateLost:
			w.logf("job %s lost its lease; result dropped", job.ID)
		}
	case <-timeout:
		cancel()
		finish("/fail", failBody("Timeout", fmt.Sprintf("exceeded %ds", *job.TimeoutSecs), true))
	case out := <-done:
		switch {
		case state.Load() == stateLost:
			w.logf("job %s lost its lease; result dropped", job.ID)
		case state.Load() == stateAborted:
			finish("/fail", map[string]any{"aborted": true})
		case out.err != nil:
			var nr nonRetryable
			retryable := !errors.As(out.err, &nr)
			finish("/fail", map[string]any{"error": errorBody(out.err), "retryable": retryable})
		default:
			result, err := json.Marshal(out.value)
			if err != nil {
				finish("/fail", failBody("UnserializableResult", err.Error(), false))
				return
			}
			finish("/complete", map[string]any{"result": json.RawMessage(result)})
		}
	}
}

// report sends complete/fail, retrying for at least a lease TTL: the job's effects
// already happened (spec §6).
func (w *RemoteWorker) report(job Job, path string, body map[string]any) {
	deadline := time.Now().Add(time.Duration(max(job.LeaseTTL, 30)) * time.Second)
	delay := 500 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _, err := w.call(ctx, "/leases/"+job.LeaseID+path, w.workerToken, body)
		cancel()
		if err == nil {
			return
		}
		var pe *ProtocolError
		if errors.As(err, &pe) {
			switch {
			case pe.Code == "too_large" && path == "/complete":
				path, body = "/fail", failBody("ResultTooLarge", pe.Message, false)
				continue
			case pe.Code == "lease_lost":
				w.logf("job %s lost its lease; result dropped", job.ID)
				return
			case pe.Status == 401:
				w.revoke()
				return
			case !pe.Retryable():
				w.logf("reporting job %s failed: %v", job.ID, err)
				return
			}
		}
		if time.Now().After(deadline) {
			w.logf("giving up reporting job %s", job.ID)
			return
		}
		time.Sleep(delay)
		delay = min(delay*2, 10*time.Second)
	}
}

// --- helpers -------------------------------------------------------------------

func decodeClaim(raw []byte) (Job, error) {
	var c struct {
		Job struct {
			ID          string            `json:"id"`
			Function    string            `json:"function"`
			Queue       string            `json:"queue"`
			Kwargs      json.RawMessage   `json:"kwargs"`
			Args        []json.RawMessage `json:"args"`
			Attempts    int               `json:"attempts"`
			MaxAttempts int               `json:"max_attempts"`
			LostCount   int               `json:"lost_count"`
			TimeoutSecs *int              `json:"timeout_secs"`
			Meta        json.RawMessage   `json:"meta"`
		} `json:"job"`
		LeaseID        string `json:"lease_id"`
		LeaseTTL       int    `json:"lease_ttl"`
		HeartbeatEvery int    `json:"heartbeat_every"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return Job{}, err
	}
	j := c.Job
	return Job{
		ID: j.ID, Function: j.Function, Queue: j.Queue, Kwargs: j.Kwargs, Args: j.Args,
		Attempts: j.Attempts, MaxAttempts: j.MaxAttempts, LostCount: j.LostCount, TimeoutSecs: j.TimeoutSecs,
		Meta: j.Meta, LeaseID: c.LeaseID, LeaseTTL: c.LeaseTTL, HeartbeatEvery: c.HeartbeatEvery,
	}, nil
}

func failBody(kind, message string, retryable bool) map[string]any {
	return map[string]any{"error": map[string]any{"type": kind, "message": message}, "retryable": retryable}
}

func errorBody(err error) map[string]any {
	var nr nonRetryable
	if errors.As(err, &nr) {
		err = nr.err
	}
	kind := fmt.Sprintf("%T", err)
	if t, ok := err.(interface{ Type() string }); ok {
		kind = t.Type()
	} else if strings.HasPrefix(kind, "*errors.") || strings.HasPrefix(kind, "*fmt.") {
		kind = "Error" // errors.New / fmt.Errorf: the concrete type is a stdlib detail
	} else {
		kind = strings.TrimPrefix(kind, "*")
	}
	return map[string]any{"type": kind, "message": err.Error()}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
