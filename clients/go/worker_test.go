package pgwerk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type call struct {
	Path    string
	Body    map[string]any
	Headers http.Header
}

func fakeServer(t *testing.T, respond func(path string, n int) (int, string)) (*RemoteWorker, *[]call) {
	t.Helper()
	var mu sync.Mutex
	calls := []call{}
	counts := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		path := strings.TrimPrefix(r.URL.Path, "/remote/v1")
		mu.Lock()
		calls = append(calls, call{path, body, r.Header.Clone()})
		counts[path]++
		n := counts[path]
		mu.Unlock()
		status, resp := respond(path, n)
		rw.WriteHeader(status)
		_, _ = rw.Write([]byte(resp))
	}))
	t.Cleanup(srv.Close)
	w, err := NewRemoteWorker(srv.URL, "pgwk_e_t", Queues("q"), Insecure(), Logger(nil))
	if err != nil {
		t.Fatal(err)
	}
	w.workerToken = "pgwk_w_t"
	return w, &calls
}

func protoErr(code string) string { return `{"error":{"code":"` + code + `","message":"x"}}` }

var testJob = Job{ID: "j", LeaseID: "L", LeaseTTL: 1, HeartbeatEvery: 1, Kwargs: json.RawMessage(`{}`)}

func TestRefusesPlainHTTPUnlessInsecure(t *testing.T) {
	if _, err := NewRemoteWorker("http://x", "t", Queues("q")); err == nil {
		t.Fatal("expected an error for http://")
	}
	if _, err := NewRemoteWorker("http://x", "t", Queues("q"), Insecure()); err != nil {
		t.Fatal(err)
	}
}

func TestReportSendsHeadersAndRetriesUntilAccepted(t *testing.T) {
	w, calls := fakeServer(t, func(_ string, n int) (int, string) {
		if n < 3 {
			return 503, protoErr("internal")
		}
		return 200, `{"status":"complete"}`
	})
	w.report(testJob, "/complete", map[string]any{"result": 1})
	if len(*calls) != 3 {
		t.Fatalf("want 3 calls, got %d", len(*calls))
	}
	c := (*calls)[0]
	if c.Headers.Get("Pgwerk-Protocol") != "1" || c.Headers.Get("Authorization") != "Bearer pgwk_w_t" {
		t.Fatalf("bad headers: %v", c.Headers)
	}
}

func TestReportStopsOnLeaseLost(t *testing.T) {
	w, calls := fakeServer(t, func(string, int) (int, string) { return 409, protoErr("lease_lost") })
	w.report(testJob, "/complete", map[string]any{"result": 1})
	if len(*calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(*calls))
	}
}

func TestTooLargeBecomesNonRetryableFailure(t *testing.T) {
	w, calls := fakeServer(t, func(path string, _ int) (int, string) {
		if strings.HasSuffix(path, "complete") {
			return 413, protoErr("too_large")
		}
		return 200, `{}`
	})
	w.report(testJob, "/complete", map[string]any{"result": "x"})
	fail := (*calls)[1]
	if fail.Path != "/leases/L/fail" || fail.Body["retryable"] != false {
		t.Fatalf("bad fail: %+v", fail)
	}
	if fail.Body["error"].(map[string]any)["type"] != "ResultTooLarge" {
		t.Fatalf("bad error: %+v", fail.Body)
	}
}

func TestUnauthorizedRevokes(t *testing.T) {
	w, _ := fakeServer(t, func(string, int) (int, string) { return 401, protoErr("unauthorized") })
	w.report(testJob, "/complete", map[string]any{"result": 1})
	if !w.Revoked() {
		t.Fatal("expected the worker to be revoked")
	}
}

func TestHandleDecodesKwargsAndReportsTheResult(t *testing.T) {
	w, calls := fakeServer(t, func(string, int) (int, string) { return 200, `{"status":"complete"}` })
	type In struct{ A, B int }
	Handle(w, "add", func(_ Context, in In) (int, error) { return in.A + in.B, nil })
	job := testJob
	job.Function, job.Kwargs = "add", json.RawMessage(`{"a":2,"b":3}`)
	w.runJob(job)
	last := (*calls)[len(*calls)-1]
	if last.Path != "/leases/L/complete" || last.Body["result"] != float64(5) {
		t.Fatalf("bad complete: %+v", last)
	}
}

func TestNonRetryableAndArgs(t *testing.T) {
	w, calls := fakeServer(t, func(string, int) (int, string) { return 200, `{"status":"failed"}` })
	Handle(w, "nope", func(_ Context, _ struct{}) (any, error) { return nil, NonRetryable(errors.New("bad")) })
	job := testJob
	job.Function = "nope"
	w.runJob(job)
	if (*calls)[0].Body["retryable"] != false {
		t.Fatalf("NonRetryable should not retry: %+v", (*calls)[0].Body)
	}
	job.Args = []json.RawMessage{json.RawMessage(`1`)}
	w.runJob(job)
	errBody := (*calls)[1].Body["error"].(map[string]any)
	if errBody["type"] != "UnsupportedPayload" || (*calls)[1].Body["retryable"] != false {
		t.Fatalf("args job should fail with UnsupportedPayload: %+v", (*calls)[1].Body)
	}
}

func TestRunNeedsAHandler(t *testing.T) {
	w, _ := fakeServer(t, func(string, int) (int, string) { return 200, `{}` })
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected an error without handlers")
	}
}

type quotaError struct{}

func (quotaError) Error() string { return "over quota" }

func TestErrorTypes(t *testing.T) {
	cases := map[string]error{
		"Error":             errors.New("x"),
		"pgwerk.quotaError": quotaError{},
	}
	for want, err := range cases {
		if got := errorBody(err)["type"]; got != want {
			t.Errorf("errorBody(%T) type = %v, want %v", err, got, want)
		}
	}
	if got := errorBody(NonRetryable(quotaError{}))["type"]; got != "pgwerk.quotaError" {
		t.Errorf("NonRetryable should report the wrapped type, got %v", got)
	}
}

func TestRefusesAnythingButHTTPSRegardlessOfCase(t *testing.T) {
	for _, u := range []string{"HTTP://x", "Http://x", "ftp://x", "x"} {
		if _, err := NewRemoteWorker(u, "t", Queues("q")); err == nil {
			t.Fatalf("expected an error for %q", u)
		}
	}
	if _, err := NewRemoteWorker("HTTPS://x", "t", Queues("q")); err != nil {
		t.Fatal(err)
	}
}

func TestNeverFollowsRedirects(t *testing.T) {
	w, err := NewRemoteWorker("https://x", "t", Queues("q"))
	if err != nil {
		t.Fatal(err)
	}
	if w.http.CheckRedirect == nil {
		t.Fatal("the HTTP client must refuse redirects (the bearer token would follow them)")
	}
	req, _ := http.NewRequest(http.MethodPost, "https://x/remote/v1/claim", nil)
	if err := w.http.CheckRedirect(req, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
	custom := &http.Client{}
	w, _ = NewRemoteWorker("https://x", "t", Queues("q"), HTTPClient(custom))
	if w.http.CheckRedirect == nil {
		t.Fatal("a caller's client must not follow redirects either")
	}
}
