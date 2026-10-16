package tmdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/go-resty/resty/v2"
)

// TapeKindRequest is the observation kind recorded for a TMDB API call.
const TapeKindRequest = "tmdb.request"

// Error kinds recorded for a failed TMDB call. They exist because the
// classifier's control flow depends on which of these it got: find_match falls
// through on an unmatched result only, an unauthorized response latches a
// process-lifetime failure, and everything else is fatal to the classification.
// A replay that flattened them into one error would change that control flow.
const (
	TapeErrorKindUnauthorized = "unauthorized"
	TapeErrorKindNotFound     = "not_found"
	TapeErrorKindHTTP         = "http"
	TapeErrorKindTransport    = "transport"
)

// tapeRequest is the question asked of the TMDB API.
//
// QueryParams is recorded in full because it is where a port's mistakes show
// up: a missing year, a dropped include_adult, a differently rendered id. The
// api_key is not part of it -- it is set once on the HTTP client and never
// passes through here -- so a tape carries no credential.
type tapeRequest struct {
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	QueryParams map[string]string `json:"queryParams"`
}

// tapeResponse holds the response exactly as it arrived. The body is recorded
// as raw bytes rather than re-encoded JSON so a replay feeds its deserializer
// the same input the recording did, down to key order and whitespace.
type tapeResponse struct {
	StatusCode int    `json:"statusCode"`
	Status     string `json:"status"`
	BodyBase64 string `json:"bodyBase64"`
	BodySHA256 string `json:"bodySha256"`
}

// requesterRecorder is the TMDB tape seam.
//
// It sits at the top of the requester chain, above the lazy initialisation, so
// that a replay never builds a live requester: no API key is needed, no
// validation call is made, and no request leaves the process.
//
// With no tape session on the context -- every normally configured process --
// it delegates and returns, adding a context lookup and a nil check.
type requesterRecorder struct {
	requester Requester
}

func (r requesterRecorder) Request(
	ctx context.Context,
	path string,
	queryParams map[string]string,
	result any,
) (*resty.Response, error) {
	session := tape.SessionFrom(ctx)
	if session == nil {
		return r.requester.Request(ctx, path, queryParams, result)
	}

	request := newTapeRequest(path, queryParams)

	if session.Replaying() {
		return nil, replayRequest(session, request, result)
	}

	res, err := r.requester.Request(ctx, path, queryParams, result)
	if err != nil {
		session.ObserveErrorDetail(
			TapeKindRequest,
			request,
			requestErrorKind(res, err),
			err.Error(),
			newTapeResponse(res),
		)

		return res, err
	}

	session.Observe(TapeKindRequest, request, newTapeResponse(res))

	return res, nil
}

func newTapeRequest(path string, queryParams map[string]string) tapeRequest {
	// Always a non-nil map: an absent parameter set and an empty one must encode
	// identically so a replay's request comparison is about the parameters and
	// not about how the caller happened to spell "none".
	params := make(map[string]string, len(queryParams))
	for key, value := range queryParams {
		params[key] = value
	}

	return tapeRequest{Method: "GET", Path: path, QueryParams: params}
}

func newTapeResponse(res *resty.Response) tapeResponse {
	if res == nil {
		return tapeResponse{BodySHA256: hashBody(nil)}
	}

	body := res.Body()

	return tapeResponse{
		StatusCode: res.StatusCode(),
		Status:     res.Status(),
		BodyBase64: base64.StdEncoding.EncodeToString(body),
		BodySHA256: hashBody(body),
	}
}

func hashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// requestErrorKind classifies an error by the sentinel the caller will test
// for, not by its text.
func requestErrorKind(res *resty.Response, err error) string {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return TapeErrorKindUnauthorized
	case errors.Is(err, ErrNotFound):
		return TapeErrorKindNotFound
	case res != nil && !res.IsSuccess():
		return TapeErrorKindHTTP
	default:
		return TapeErrorKindTransport
	}
}

func replayRequest(session *tape.Session, request tapeRequest, result any) error {
	responseJSON, observationErr, err := session.Next(TapeKindRequest, request)
	if err != nil {
		return err
	}

	if observationErr != nil {
		return rebuildRequestError(observationErr)
	}

	var response tapeResponse
	if err := json.Unmarshal(responseJSON, &response); err != nil {
		return fmt.Errorf("decode taped %s response: %w", TapeKindRequest, err)
	}

	body, err := base64.StdEncoding.DecodeString(response.BodyBase64)
	if err != nil {
		return fmt.Errorf("decode taped %s response body: %w", TapeKindRequest, err)
	}

	if got := hashBody(body); got != response.BodySHA256 {
		return fmt.Errorf(
			"taped %s response body digest is %s, but the recorded digest is %s",
			TapeKindRequest, got, response.BodySHA256,
		)
	}

	if result == nil {
		// ValidateAPIKey discards the body; a successful observation is the
		// whole answer.
		return nil
	}

	// Unmarshal through a pointer to the interface, mirroring how the live
	// requester hands `result` to resty.
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode taped %s response body: %w", TapeKindRequest, err)
	}

	return nil
}

// rebuildRequestError turns a recorded failure back into the value the live
// requester would have returned. The unauthorized and not-found cases return
// the package sentinels themselves, because callers reach them with errors.Is
// and a look-alike would not match.
func rebuildRequestError(observationErr *tape.ObservationError) error {
	switch observationErr.Kind {
	case TapeErrorKindUnauthorized:
		return ErrUnauthorized
	case TapeErrorKindNotFound:
		return ErrNotFound
	default:
		return errors.New(observationErr.Message)
	}
}
