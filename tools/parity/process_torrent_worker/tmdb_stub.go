package main

// A stub of the TMDB v3 API for the flags-on differential. Go (tmdb.base_url)
// and the Rust worker (--tmdb-base-url) are both pointed at it.
//
// Responses are keyed by the request -- method, path below the base path, and
// the query string with api_key removed, re-encoded canonically -- and never by
// arrival order, so the two workers' different concurrency cannot change an
// answer. A request without a fixture gets TMDB's own answer for an unknown
// resource: an empty result page for a search or a find, a 404 for details.
//
// Every request is appended to a JSONL log, one line per HTTP request (each
// client retry is its own line), so the request multisets of two runs can be
// compared with `request-diff`.
//
// A fault fixture names either one fault, answered every time, or a sequence
// of steps answered in turn per request key, the last step repeating: ["500",
// "ok"] fails the first request for its key and answers the second normally.
// A per-key count is deterministic only when one torrent owns the key, which
// the fault corpora guarantee.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Bodies TMDB returns for its error and empty cases, verbatim in shape.
const (
	tmdbEmptySearchBody = `{"page":1,"results":[],"total_pages":1,"total_results":0}`
	tmdbEmptyFindBody   = `{"movie_results":[],"person_results":[],"tv_results":[],` +
		`"tv_episode_results":[],"tv_season_results":[]}`
	tmdbNotFoundBody = `{"success":false,"status_code":34,` +
		`"status_message":"The resource you requested could not be found."}`
	tmdbUnauthorizedBody = `{"status_code":7,` +
		`"status_message":"Invalid API key: You must be granted a valid key.","success":false}`
	tmdbAuthenticationBody = `{"success":true,"status_code":1,"status_message":"Success."}`
	tmdbRateLimitedBody    = `{"status_code":25,` +
		`"status_message":"Your request count (41) is over the allowed limit of (40)."}`
	tmdbInternalErrorBody = `{"status_code":11,` +
		`"status_message":"Internal error: Something went wrong, contact TMDb."}`
	tmdbUnavailableBody = `{"status_code":9,` +
		`"status_message":"Service offline: This service is temporarily offline, try again later."}`
	tmdbContentType = "application/json;charset=utf-8"
)

// Fault kinds a fault fixture can name.
const (
	faultUnauthorized = "401"
	faultNotFound     = "404"
	faultRateLimited  = "429"
	faultInternal     = "500"
	faultUnavailable  = "503"
	faultTimeout      = "timeout"
	// faultReset reads the request, then resets the connection (SO_LINGER 0).
	faultReset = "reset"

	// stepOK is a sequence step answered as if the key had no fault.
	stepOK = "ok"

	// sourceFault is stubLogEntry.Source for an answer a fault fixture gave.
	sourceFault = "fault"
)

// isTransportFault reports whether a fault kind fails below HTTP, where both
// clients see an error rather than a status.
func isTransportFault(kind string) bool { return kind == faultTimeout || kind == faultReset }

// Answers that are not an HTTP status: abort the connection (a timeout fault),
// or reset it.
const (
	statusAbort = 0
	statusReset = -1
)

// stubResponse is one line of the responses fixture: the canonical request key
// and what to answer it with.
type stubResponse struct {
	Key    string          `json:"key"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// stubFault is one line of the faults fixture: Fault, answered to every
// request for Key, or Sequence, answered step by step.
type stubFault struct {
	Key      string   `json:"key"`
	Fault    string   `json:"fault,omitempty"`
	Sequence []string `json:"sequence,omitempty"`
}

// steps is the fault as a sequence.
func (f stubFault) steps() ([]string, error) {
	switch {
	case f.Fault != "" && len(f.Sequence) == 0:
		return []string{f.Fault}, nil
	case f.Fault == "" && len(f.Sequence) > 0:
		return f.Sequence, nil
	default:
		return nil, fmt.Errorf("fault %q: want exactly one of fault and sequence", f.Key)
	}
}

// stubLogEntry is one line of the request log. Seq, Elapsed and Step are the
// only order- or time-dependent fields; request-diff ignores them. Seq numbers
// the answers in the order they are decided (just before one is written, or
// when a timeout gives up), which is the order of the lines.
type stubLogEntry struct {
	Seq       int64             `json:"seq"`
	Elapsed   string            `json:"elapsed"`
	Step      int               `json:"step,omitempty"` // 1-based, for keys with a fault fixture
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Query     map[string]string `json:"query"`
	Key       string            `json:"key"`
	APIKeyOK  bool              `json:"apiKeyOk"`
	RawQuery  string            `json:"rawQuery"`
	Status    int               `json:"status"`
	Source    string            `json:"source"`
	Endpoint  string            `json:"endpoint"`
	FaultKind string            `json:"fault,omitempty"`
}

type tmdbStub struct {
	basePath     string
	apiKey       string
	timeoutDelay time.Duration
	responses    map[string]stubResponse
	faults       map[string][]string
	started      time.Time

	seq atomic.Int64

	mu     sync.Mutex
	served map[string]int // requests per fault key so far
	log    io.Writer
}

// requestKey renders the canonical fixture key of a request: the method, the
// path below the base path, and the query with api_key removed, re-encoded by
// url.Values.Encode (sorted keys, one canonical escaping).
func requestKey(method, path string, query url.Values) string {
	q := url.Values{}

	for k, vs := range query {
		if k == "api_key" {
			continue
		}

		q[k] = vs
	}

	if encoded := q.Encode(); encoded != "" {
		return method + " " + path + "?" + encoded
	}

	return method + " " + path
}

// TMDB paths that name their own endpoint family.
const (
	endpointAuthentication = "/authentication"
	endpointSearchMovie    = "/search/movie"
	endpointSearchTV       = "/search/tv"
)

// endpointOf names the TMDB endpoint family of a path, for counting.
func endpointOf(path string) string {
	switch {
	case path == endpointAuthentication:
		return endpointAuthentication
	case path == endpointSearchMovie, path == endpointSearchTV:
		return path
	case strings.HasPrefix(path, "/movie/"):
		return "/movie/{id}"
	case strings.HasPrefix(path, "/tv/"):
		return "/tv/{id}"
	case strings.HasPrefix(path, "/find/"):
		return "/find"
	default:
		return "other"
	}
}

func (s *tmdbStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, s.basePath)
	query := r.URL.Query()
	key := requestKey(r.Method, path, query)
	apiKeyOK := query.Get("api_key") == s.apiKey && len(query["api_key"]) == 1

	entry := stubLogEntry{
		Method:   r.Method,
		Path:     path,
		Query:    flatQuery(query),
		Key:      key,
		APIKeyOK: apiKeyOK,
		RawQuery: r.URL.RawQuery,
		Endpoint: endpointOf(path),
	}

	status, body := s.answer(r.Context(), path, key, apiKeyOK, &entry)
	entry.Status = status

	s.record(entry)

	switch status {
	case statusAbort:
		// A timeout fault. Returning normally would send an implicit empty
		// 200; abort the connection instead, which is what a client that held
		// on past -timeout-delay sees too.
		panic(http.ErrAbortHandler)
	case statusReset:
		resetConnection(w)
		return
	}

	w.Header().Set("Content-Type", tmdbContentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *tmdbStub) answer(
	ctx context.Context,
	path, key string,
	apiKeyOK bool,
	entry *stubLogEntry,
) (int, []byte) {
	if steps, ok := s.faults[key]; ok {
		s.mu.Lock()
		n := s.served[key]
		s.served[key] = n + 1
		s.mu.Unlock()

		entry.Step = n + 1

		if step := steps[min(n, len(steps)-1)]; step != stepOK {
			entry.Source = sourceFault
			entry.FaultKind = step

			return s.fault(ctx, step)
		}
	}

	if !apiKeyOK {
		entry.Source = "bad-api-key"

		return http.StatusUnauthorized, []byte(tmdbUnauthorizedBody)
	}

	if response, ok := s.responses[key]; ok {
		entry.Source = "fixture"

		return response.Status, response.Body
	}

	entry.Source = "default"

	switch endpointOf(path) {
	case endpointAuthentication:
		return http.StatusOK, []byte(tmdbAuthenticationBody)
	case endpointSearchMovie, endpointSearchTV:
		return http.StatusOK, []byte(tmdbEmptySearchBody)
	case "/find":
		return http.StatusOK, []byte(tmdbEmptyFindBody)
	default:
		return http.StatusNotFound, []byte(tmdbNotFoundBody)
	}
}

func (s *tmdbStub) fault(ctx context.Context, fault string) (int, []byte) {
	switch fault {
	case faultUnauthorized:
		return http.StatusUnauthorized, []byte(tmdbUnauthorizedBody)
	case faultNotFound:
		return http.StatusNotFound, []byte(tmdbNotFoundBody)
	case faultRateLimited:
		return http.StatusTooManyRequests, []byte(tmdbRateLimitedBody)
	case faultInternal:
		return http.StatusInternalServerError, []byte(tmdbInternalErrorBody)
	case faultUnavailable:
		return http.StatusServiceUnavailable, []byte(tmdbUnavailableBody)
	case faultTimeout:
		select {
		case <-ctx.Done():
		case <-time.After(s.timeoutDelay):
		}

		return statusAbort, nil
	case faultReset:
		return statusReset, nil
	default:
		return http.StatusInternalServerError, []byte(tmdbInternalErrorBody)
	}
}

// resetConnection takes the connection from the server and closes it with
// SO_LINGER 0, so the client, waiting for its answer, reads ECONNRESET.
func resetConnection(w http.ResponseWriter) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	_ = conn.Close()
}

func (s *tmdbStub) record(entry stubLogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry.Seq = s.seq.Add(1)
	entry.Elapsed = time.Since(s.started).Round(time.Millisecond).String()

	line, err := json.Marshal(entry)
	if err != nil {
		panic(err)
	}

	_, _ = s.log.Write(append(line, '\n'))
}

func flatQuery(query url.Values) map[string]string {
	flat := make(map[string]string, len(query))

	for k, vs := range query {
		if k == "api_key" {
			continue
		}

		flat[k] = strings.Join(vs, "\x00")
	}

	return flat
}

// stubFaultSteps indexes the fault fixtures' steps by key, with authFault,
// when set, answered to every /authentication request.
func stubFaultSteps(faults []stubFault, authFault string) (map[string][]string, error) {
	out := make(map[string][]string, len(faults)+1)

	for _, f := range faults {
		steps, err := f.steps()
		if err != nil {
			return nil, err
		}

		if _, dup := out[f.Key]; dup {
			return nil, fmt.Errorf("faults: duplicate key %q", f.Key)
		}

		out[f.Key] = steps
	}

	if authFault != "" {
		key := requestKey(http.MethodGet, endpointAuthentication, nil)
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("faults: -authentication-fault and a fixture both fault %q", key)
		}

		out[key] = []string{authFault}
	}

	return out, nil
}

func readStubJSONL[T any](path string) ([]T, error) {
	if path == "" {
		return nil, nil
	}

	return readJSONL[T](path, 0)
}

func runTMDBStub(args []string) error {
	fs := flag.NewFlagSet("tmdb-stub", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "listen address")
	basePath := fs.String("base-path", "/3", "path prefix the clients' base URL carries")
	apiKey := fs.String("api-key", "", "the only api_key the stub accepts")
	responsesPath := fs.String("responses", "", "responses fixture (JSONL)")
	faultsPath := fs.String("faults", "", "faults fixture (JSONL)")
	logPath := fs.String("log", "", "request log (JSONL), appended")
	timeoutDelay := fs.Duration("timeout-delay", 2*time.Minute,
		"how long a timeout fault holds a request before aborting it (both clients give up after 10s)")
	readyPath := fs.String("ready", "", "file written with the listen address once serving")
	keepAlive := fs.Bool("keepalive", true, "HTTP keep-alive; off for transport faults, since on a reused "+
		"connection net/http resends an idempotent request that met a reset, so counts would follow pool state")
	authFault := fs.String("authentication-fault", "", "a fault kind answered to every /authentication "+
		"request, failing both clients' key validation")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *apiKey == "" || *logPath == "" {
		return errors.New("tmdb-stub: -api-key and -log are required")
	}

	responses, err := readStubJSONL[stubResponse](*responsesPath)
	if err != nil {
		return fmt.Errorf("responses: %w", err)
	}

	faults, err := readStubJSONL[stubFault](*faultsPath)
	if err != nil {
		return fmt.Errorf("faults: %w", err)
	}

	steps, err := stubFaultSteps(faults, *authFault)
	if err != nil {
		return err
	}

	stub := &tmdbStub{
		basePath:     *basePath,
		apiKey:       *apiKey,
		timeoutDelay: *timeoutDelay,
		responses:    make(map[string]stubResponse, len(responses)),
		faults:       steps,
		served:       map[string]int{},
		started:      time.Now(),
	}

	for _, r := range responses {
		if _, dup := stub.responses[r.Key]; dup {
			return fmt.Errorf("responses: duplicate key %q", r.Key)
		}

		stub.responses[r.Key] = r
	}

	logFile, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()

	buffered := bufio.NewWriterSize(logFile, 1<<16)
	stub.log = buffered

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}

	server := &http.Server{Handler: stub, ReadHeaderTimeout: 10 * time.Second}
	server.SetKeepAlivesEnabled(*keepAlive)

	if *readyPath != "" {
		if err := os.WriteFile(*readyPath, []byte(listener.Addr().String()+"\n"), 0o644); err != nil {
			return err
		}
	}

	_, _ = fmt.Fprintf(os.Stderr, "tmdb-stub: %d responses, %d faults, listening on %s\n",
		len(stub.responses), len(stub.faults), listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)

	go func() { errc <- server.Serve(listener) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Shutdown(shutdownCtx)

	stub.mu.Lock()
	defer stub.mu.Unlock()

	return buffered.Flush()
}

// requestCount is one row of a request multiset.
type requestCount struct {
	key   string
	count int
}

// requestMultiset counts a request log's lines by (key, status, source, api key
// validity), the order- and time-free identity of a request and its answer,
// optionally leaving one endpoint family out.
func requestMultiset(path, ignoreEndpoint string) (multiset, endpoints map[string]int, err error) {
	entries, err := readJSONL[stubLogEntry](path, 0)
	if err != nil {
		return nil, nil, err
	}

	multiset = make(map[string]int, len(entries))
	endpoints = make(map[string]int)

	for _, e := range entries {
		if ignoreEndpoint != "" && e.Endpoint == ignoreEndpoint {
			continue
		}

		id := fmt.Sprintf("%s | status=%d | source=%s | apiKeyOk=%t", e.Key, e.Status, e.Source, e.APIKeyOK)
		multiset[id]++
		endpoints[fmt.Sprintf("%s %d", e.Endpoint, e.Status)]++
	}

	return multiset, endpoints, nil
}

func sortedCounts(m map[string]int) []requestCount {
	out := make([]requestCount, 0, len(m))
	for k, v := range m {
		out = append(out, requestCount{k, v})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })

	return out
}

// runRequestDiff compares two request logs as multisets and prints every
// difference; it fails when they differ.
func runRequestDiff(args []string) error {
	fs := flag.NewFlagSet("request-diff", flag.ExitOnError)
	ignore := fs.String("ignore-endpoint", "", "leave one endpoint family out, e.g. /authentication when "+
		"the two sides ran a different number of worker processes")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() != 2 {
		return errors.New("usage: request-diff [-ignore-endpoint E] A.jsonl B.jsonl")
	}

	a, aEndpoints, err := requestMultiset(fs.Arg(0), *ignore)
	if err != nil {
		return err
	}

	b, bEndpoints, err := requestMultiset(fs.Arg(1), *ignore)
	if err != nil {
		return err
	}

	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}

	for k := range b {
		keys[k] = struct{}{}
	}

	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}

	sort.Strings(sorted)

	differences, total := 0, 0

	for _, k := range sorted {
		total += a[k]
		if a[k] != b[k] {
			differences++
			_, _ = fmt.Fprintf(os.Stdout, "%+d\t%d\t%d\t%s\n", b[k]-a[k], a[k], b[k], k)
		}
	}

	for _, c := range sortedCounts(aEndpoints) {
		fmt.Fprintf(os.Stderr, "A endpoint %s: %d (B %d)\n", c.key, c.count, bEndpoints[c.key])
	}

	for _, c := range sortedCounts(bEndpoints) {
		if _, ok := aEndpoints[c.key]; !ok {
			fmt.Fprintf(os.Stderr, "B-only endpoint %s: %d\n", c.key, c.count)
		}
	}

	if differences > 0 {
		return fmt.Errorf("request multisets DIFFER: %d distinct requests differ (A has %d requests)",
			differences, total)
	}

	fmt.Fprintf(os.Stderr, "request multisets IDENTICAL: %d requests, %d distinct\n", total, len(sorted))

	return nil
}
