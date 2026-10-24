package tmdb

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const testAPIKey = "m2c-dummy-key-0123456789abcdef"

func TestRedactAPIKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{
			name: "first parameter",
			in:   "https://api.themoviedb.org/3/search/movie?api_key=abc123&query=x",
			want: "https://api.themoviedb.org/3/search/movie?api_key=REDACTED&query=x",
		},
		{
			name: "later parameter",
			in:   "https://h/3/movie/1?append_to_response=credits&api_key=abc123",
			want: "https://h/3/movie/1?append_to_response=credits&api_key=REDACTED",
		},
		{
			name: "escaped value",
			in:   "https://h/3/x?api_key=a%26b%22c&query=x",
			want: "https://h/3/x?api_key=REDACTED&query=x",
		},
		{
			name: "empty value",
			in:   "https://h/3/x?api_key=&query=x",
			want: "https://h/3/x?api_key=REDACTED&query=x",
		},
		{
			name: "fragment",
			in:   "https://h/3/x?api_key=abc123#top",
			want: "https://h/3/x?api_key=REDACTED#top",
		},
		{
			name: "quoted in an attempt warning",
			in:   `Get "http://h/3/x?api_key=abc123&query=x": dial tcp: connection refused, Attempt 2`,
			want: `Get "http://h/3/x?api_key=REDACTED&query=x": dial tcp: connection refused, Attempt 2`,
		},
		{
			name: "every occurrence",
			in:   "a?api_key=k1 b?api_key=k2",
			want: "a?api_key=REDACTED b?api_key=REDACTED",
		},
		{
			name: "other parameters",
			in:   "https://h/3/x?my_api_key=k1&api_keys=k2&xapi_key=k3",
			want: "https://h/3/x?my_api_key=k1&api_keys=k2&xapi_key=k3",
		},
		{
			name: "status error",
			in:   "TMDB request failed: 500 Internal Server Error",
			want: "TMDB request failed: 500 Internal Server Error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, redactAPIKey(tc.in))
		})
	}
}

// A transport failure must reach the caller, resty's own log lines and the
// requester logger without the api_key, and otherwise exactly as net/http
// rendered it.
func TestTransportErrorsDoNotCarryTheAPIKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		timeout bool
	}{
		{
			name: "timeout",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			},
			timeout: true,
		},
		{
			name:    "connection reset",
			handler: resetConnection,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var keyed atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("api_key") == testAPIKey {
					keyed.Add(1)
				}

				tc.handler(w, r)
			}))
			defer server.Close()

			core, logs := observer.New(zap.DebugLevel)
			requester := testRequester(server.URL, zap.New(core).Sugar())

			var result SearchMovieResponse
			_, err := requester.Request(
				context.Background(),
				"/search/movie",
				map[string]string{"query": "x", "include_adult": "true"},
				&result,
			)

			// Both attempts went out with the real key.
			require.Equal(t, int32(2), keyed.Load())

			var urlErr *url.Error
			require.ErrorAs(t, err, &urlErr)

			wantURL := server.URL + "/3/search/movie?api_key=REDACTED&include_adult=true&query=x"
			require.Equal(t, wantURL, urlErr.URL)
			require.EqualError(t, err, `Get "`+wantURL+`": `+urlErr.Err.Error())
			require.Equal(t, tc.timeout, urlErr.Timeout())

			// resty warns once per failed attempt and logs the final error;
			// the requester logger then logs the request.
			entries := logs.All()
			require.Len(t, entries, 4)

			for i, entry := range entries[:2] {
				require.Equal(t, zap.WarnLevel, entry.Level)
				require.True(t, strings.HasPrefix(entry.Message, `Get "`+wantURL+`": `), entry.Message)
				require.True(t, strings.HasSuffix(entry.Message, fmt.Sprintf(", Attempt %d", i+1)), entry.Message)
			}

			require.Equal(t, zap.ErrorLevel, entries[2].Level)
			require.Equal(t, err.Error(), entries[2].Message)
			require.Equal(t, "request failed", entries[3].Message)
			require.Equal(t, err.Error(), entries[3].ContextMap()["error"])

			for _, entry := range entries {
				require.NotContains(t, entry.Message, testAPIKey)
				require.NotContains(t, fmt.Sprint(entry.ContextMap()), testAPIKey)
			}
		})
	}
}

// Status errors carry no URL, so they keep their exact text and identity.
func TestStatusErrorsAreUnchanged(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/3/unauthorized":
			w.WriteHeader(http.StatusUnauthorized)
		case "/3/not-found":
			w.WriteHeader(http.StatusNotFound)
		case "/3/server-error":
			w.WriteHeader(http.StatusInternalServerError)
		case "/3/custom-phrase":
			writeRawStatusLine(w, "HTTP/1.1 599 Custom Phrase")
		}
	}))
	defer server.Close()

	core, logs := observer.New(zap.DebugLevel)
	requester := testRequester(server.URL, zap.New(core).Sugar())

	for _, tc := range []struct {
		path     string
		want     string
		sentinel error
	}{
		{path: "/unauthorized", want: "TMDB request failed: 401 Unauthorized", sentinel: ErrUnauthorized},
		{path: "/not-found", want: "TMDB request failed: 404 Not Found", sentinel: ErrNotFound},
		{path: "/server-error", want: "TMDB request failed: 500 Internal Server Error"},
		{path: "/custom-phrase", want: "TMDB request failed: 599 Custom Phrase"},
	} {
		var result any
		_, err := requester.Request(context.Background(), tc.path, nil, &result)
		require.EqualError(t, err, tc.want, tc.path)

		if tc.sentinel != nil {
			require.ErrorIs(t, err, tc.sentinel, tc.path)
		}
	}

	// A status is an answer, not a transport failure: resty neither retries
	// nor logs it, and only the requester logger reports each request.
	require.Len(t, logs.FilterMessage("request failed").All(), 4)
	require.Len(t, logs.All(), 4)
}

// testRequester is the requester newRequester builds, minus the rate limiter
// and semaphore, with a short timeout and a single quick retry.
func testRequester(serverURL string, logger *zap.SugaredLogger) Requester {
	client := newResty(Config{BaseURL: serverURL + "/3", APIKey: testAPIKey}, logger).
		SetTimeout(200 * time.Millisecond).
		SetRetryCount(1).
		SetRetryWaitTime(time.Millisecond).
		SetRetryMaxWaitTime(time.Millisecond)

	return requesterLogger{requester: requester{resty: client}, logger: logger}
}

func resetConnection(w http.ResponseWriter, _ *http.Request) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}

	// A zero linger makes Close send a RST rather than a FIN.
	_ = conn.(*net.TCPConn).SetLinger(0)
	_ = conn.Close()
}

// writeRawStatusLine answers with a status line net/http would not write
// itself, such as a non-canonical reason phrase.
func writeRawStatusLine(w http.ResponseWriter, statusLine string) {
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}

	_, _ = buf.WriteString(statusLine + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	_ = buf.Flush()
	_ = conn.Close()
}
