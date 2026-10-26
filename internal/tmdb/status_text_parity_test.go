package tmdb

// The status text Go stores for a TMDB error answered over HTTP/2, pinned from
// both sides.
//
// HTTP/2 has no reason phrase. Go's HTTP/2 client builds res.Status from the
// :status value and its own StatusText (h2_bundle.go:9641 in go1.23.6), and
// requester.Request puts that in the error, so a code without a text is stored
// as "599 ". resty's transport sets ForceAttemptHTTP2 (transport.go:29 in
// v2.17.2, transport.go:26-29 in v2.16.5, which the prod images use), so Go
// speaks HTTP/2 to api.themoviedb.org.
//
// The Rust client rebuilds that text from statusTextFixtureRel.
// TestStatusTextFixture writes the table from this toolchain's StatusText and
// keeps it current. TestStatusTextOverHTTP2 drives the production resty client
// over HTTP/2 and checks res.Status() against the same rows.
//
// Regenerate, with the go.mod toolchain only:
//   go test ./internal/tmdb/ -run TestStatusTextFixture -update

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var updateStatusTextFixture = flag.Bool("update", false, "rewrite the Rust client's StatusText table")

const statusTextFixtureRel = "bitmagnet-rs/crates/bitmagnet-tmdb/src/go_status_text.tsv"

// The table must come from the toolchain the prod images build with, which is
// go.mod's.
func TestStatusTextFixture(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	require.Equal(t, "go"+goModVersion(t, root), runtime.Version())

	want := statusTextTable(runtime.Version())
	path := filepath.Join(root, statusTextFixtureRel)

	if *updateStatusTextFixture {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o644))
	}

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, string(got), "%s is stale; rerun with -update", statusTextFixtureRel)
}

// Every code a TMDB error can carry over HTTP/2 reaches the error as the code,
// a space and the table's text, through the production resty client.
func TestStatusTextOverHTTP2(t *testing.T) {
	t.Parallel()

	rows := readStatusTextFixture(t, filepath.Join("..", "..", statusTextFixtureRel))

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/3/status/"))
		if err != nil {
			code = http.StatusBadRequest
		}

		w.WriteHeader(code)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())

	client := newResty(Config{BaseURL: server.URL + "/3", APIKey: testAPIKey}, zap.NewNop().Sugar()).
		SetTLSClientConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
	req := requester{resty: client}

	for _, tc := range []struct {
		code     int
		sentinel error
	}{
		{code: 413},
		{code: 414},
		{code: 416},
		{code: 422},
		{code: 429},
		{code: 500},
		{code: 520},
		{code: 599},
		{code: 999},
		{code: http.StatusUnauthorized, sentinel: ErrUnauthorized},
		{code: http.StatusNotFound, sentinel: ErrNotFound},
	} {
		name := strconv.Itoa(tc.code)
		text, ok := rows[tc.code]
		require.True(t, ok, name)

		want := fmt.Sprintf("%d %s", tc.code, text)
		require.Equal(t, want, fmt.Sprintf("%d %s", tc.code, http.StatusText(tc.code)), name)

		var result any
		res, err := req.Request(context.Background(), "/status/"+name, nil, &result)
		require.NotNil(t, res, name)
		require.Equal(t, "HTTP/2.0", res.RawResponse.Proto, name)
		require.Equal(t, tc.code, res.StatusCode(), name)
		require.Equal(t, want, res.Status(), name)
		require.EqualError(t, err, "TMDB request failed: "+want, name)

		if tc.sentinel != nil {
			require.ErrorIs(t, err, tc.sentinel, name)
		}
	}

	// The TLS config replaced none of resty's transport: it is still the one
	// that forces HTTP/2.
	transport, err := client.Transport()
	require.NoError(t, err)
	require.True(t, transport.ForceAttemptHTTP2)
	require.Same(t, transport, client.GetClient().Transport)
}

func statusTextTable(goVersion string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# net/http.StatusText for every status code from 100 to 999, from %s.\n", goVersion)
	b.WriteString("# A code alone means Go has no text for it. Generated; do not edit. Regenerate with\n")
	b.WriteString("# go test ./internal/tmdb/ -run TestStatusTextFixture -update\n")

	for code := 100; code <= 999; code++ {
		if text := http.StatusText(code); text != "" {
			fmt.Fprintf(&b, "%d\t%s\n", code, text)
		} else {
			fmt.Fprintf(&b, "%d\n", code)
		}
	}

	return b.String()
}

func readStatusTextFixture(t *testing.T, path string) map[int]string {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	rows := make(map[int]string)

	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}

		code, text, _ := strings.Cut(line, "\t")
		n, err := strconv.Atoi(code)
		require.NoError(t, err, line)

		rows[n] = text
	}

	return rows
}

func goModVersion(t *testing.T, root string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	require.NoError(t, err)

	for _, line := range strings.Split(string(data), "\n") {
		if version, ok := strings.CutPrefix(line, "go "); ok {
			return strings.TrimSpace(version)
		}
	}

	t.Fatal("go.mod has no go directive")

	return ""
}
