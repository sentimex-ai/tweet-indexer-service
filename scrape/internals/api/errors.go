package twitterAPI

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// Kind says what a failed X call means for the caller. The scraper decides what
// to do from the kind alone: wait, swap the account, give up on the target, or
// stop because our own request is out of date.
type Kind int

const (
	KindUnknown       Kind = iota
	KindRateLimited        // HTTP 429 or error code 88: this account must rest
	KindSessionDead        // cookies expired or revoked (code 32, 215, 239, 401)
	KindSuspended          // our account is suspended (code 64)
	KindLocked             // our account is locked / needs a challenge (code 326, 399)
	KindTargetGone         // the profile we asked about does not exist
	KindTargetBlocked      // protected, suspended or blocking us: we cannot read its list
	KindStaleRequest       // query id or feature flags no longer accepted by X
	KindTransient          // 5xx, timeouts, connection resets: retry
)

func (k Kind) String() string {
	return [...]string{"unknown", "rate_limited", "session_dead", "suspended", "locked",
		"target_gone", "target_blocked", "stale_request", "transient"}[k]
}

// APIError is returned for every non-200 answer from X. It never carries the
// request, so it can be logged without leaking cookies or tokens.
type APIError struct {
	Kind       Kind
	Status     int
	Code       int64  // first X error code, if the body had one
	Message    string // first X error message, if the body had one
	RetryAfter time.Duration
	// MissingFeatures is filled when X says "The following features cannot be
	// null: a, b". The caller can add them and retry.
	MissingFeatures []string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("x api %s (http %d, code %d): %s", e.Kind, e.Status, e.Code, truncate(e.Message, 200))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// headerGetter is the small part of a response header we need, so the same
// classifier works for both HTTP clients in this package.
type headerGetter func(key string) string

// classify turns an HTTP status, header and body into an *APIError.
// now is passed in so tests do not depend on the clock.
func classify(status int, hdr headerGetter, body gjson.Result, now time.Time) *APIError {
	e := &APIError{Status: status, Code: body.Get("errors.0.code").Int(), Message: body.Get("errors.0.message").String()}
	if e.Message == "" {
		e.Message = body.Get("error").String()
	}
	msg := strings.ToLower(e.Message)

	if strings.HasPrefix(msg, "the following features cannot be null") {
		if i := strings.Index(e.Message, ":"); i >= 0 {
			for _, f := range strings.Split(e.Message[i+1:], ",") {
				if f = strings.TrimSpace(f); f != "" {
					e.MissingFeatures = append(e.MissingFeatures, f)
				}
			}
		}
	}

	switch {
	case status == 429 || e.Code == 88 || strings.Contains(msg, "rate limit"):
		e.Kind = KindRateLimited
		e.RetryAfter = retryAfter(hdr, now)
	case e.Code == 64 || strings.Contains(msg, "account is suspended"):
		e.Kind = KindSuspended
	case e.Code == 326 || e.Code == 399 || strings.Contains(msg, "account is locked"):
		e.Kind = KindLocked
	case e.Code == 32 || e.Code == 215 || e.Code == 239 || e.Code == 89 || status == 401 ||
		strings.Contains(msg, "could not authenticate") || strings.Contains(msg, "bad authentication"):
		e.Kind = KindSessionDead
	case e.Code == 63 || e.Code == 50 || strings.Contains(msg, "user not found") || strings.Contains(msg, "could not find user"):
		e.Kind = KindTargetGone
	case e.Code == 136 || e.Code == 179 || strings.Contains(msg, "protected") || strings.Contains(msg, "you are blocked"):
		e.Kind = KindTargetBlocked
	case len(e.MissingFeatures) > 0 || status == 400 || status == 404 || status == 422 ||
		strings.Contains(msg, "persistedquerynotfound") || strings.Contains(msg, "graphql_validation"):
		e.Kind = KindStaleRequest
	case status >= 500 || status == 0:
		e.Kind = KindTransient
	case status == 403:
		// A bare 403 with no usable code is how X answers a flagged session.
		e.Kind = KindSessionDead
	}
	return e
}

// retryAfter reads x-rate-limit-reset (unix seconds) or Retry-After (seconds).
func retryAfter(hdr headerGetter, now time.Time) time.Duration {
	if hdr == nil {
		return 0
	}
	if v := hdr("x-rate-limit-reset"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			if d := time.Unix(ts, 0).Sub(now); d > 0 {
				return d
			}
		}
	}
	if v := hdr("Retry-After"); v != "" {
		if s, err := strconv.Atoi(v); err == nil && s > 0 {
			return time.Duration(s) * time.Second
		}
	}
	return 0
}

// As is a convenience for callers: errors.As without the import.
func AsAPIError(err error) (*APIError, bool) {
	for err != nil {
		if ae, ok := err.(*APIError); ok {
			return ae, true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}
