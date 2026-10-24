package tmdb

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
)

// redactedAPIKey replaces the api_key query value wherever a request URL is
// rendered. The parameter name stays so the text still shows a key was sent.
const redactedAPIKey = "REDACTED"

// apiKeyParam matches an api_key query parameter in a URL, or in text that
// quotes one. Resty query-escapes the key, so a raw '&', '#', '"' or space
// always ends the value.
var apiKeyParam = regexp.MustCompile(`([?&]api_key=)[^&#"\s]*`)

func redactAPIKey(s string) string {
	return apiKeyParam.ReplaceAllString(s, "${1}"+redactedAPIKey)
}

// redactTransportError removes the api_key from a failed request's error.
//
// net/http reports a transport failure (a timeout, a reset, a refused
// connection) as a *url.Error whose text includes the full request URL, and
// net/http strips only a userinfo password from it, so the api_key query value
// survives into everything that renders the error: logs, tapes, health checks
// and the stored error of a queue job that fails on it. The *url.Error is
// allocated for this request alone, so its URL is rewritten in place, which
// keeps whatever wraps it and every other part of the message.
func redactTransportError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		urlErr.URL = redactAPIKey(urlErr.URL)
	}

	return err
}

// restyLogger redacts the api_key from what resty logs itself: resty logs a
// failed attempt's error, request URL included, before the requester sees it.
type restyLogger struct {
	logger *zap.SugaredLogger
}

var _ resty.Logger = restyLogger{}

func newRestyLogger(logger *zap.SugaredLogger) restyLogger {
	// Skip this wrapper so log entries keep resty's call site.
	return restyLogger{logger: logger.WithOptions(zap.AddCallerSkip(1))}
}

func (l restyLogger) Errorf(format string, v ...any) {
	l.logger.Error(redactAPIKey(fmt.Sprintf(format, v...)))
}

func (l restyLogger) Warnf(format string, v ...any) {
	l.logger.Warn(redactAPIKey(fmt.Sprintf(format, v...)))
}

func (l restyLogger) Debugf(format string, v ...any) {
	l.logger.Debug(redactAPIKey(fmt.Sprintf(format, v...)))
}
