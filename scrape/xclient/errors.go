package xclient

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// Kind says *whose* problem an error is, which decides what the caller does:
//   - account problems (RateLimited, SessionExpired, AccountSuspended,
//     AccountLocked): park/flag the pool account, retry with another one
//   - target problems (TargetNotFound/Protected/Suspended): stop, no retry
//   - config problems (StaleQuery, MissingFeatures): fix config, no point
//     rotating accounts
//   - Transient: back off and retry
type Kind int

const (
	KindUnknown Kind = iota
	KindTransient
	KindRateLimited
	KindSessionExpired
	KindAccountSuspended
	KindAccountLocked
	KindStaleQuery
	KindMissingFeatures
	KindTargetNotFound
	KindTargetProtected
	KindTargetSuspended
	KindBadRequest
	// KindForbidden is a 403 without a recognised X error code. It can be
	// one account (locked/challenged) or systemic (missing
	// x-client-transaction-id, blocked IP), so it is NOT treated as proof
	// the account is dead.
	KindForbidden
)

func (k Kind) String() string {
	switch k {
	case KindTransient:
		return "transient"
	case KindRateLimited:
		return "rate_limited"
	case KindSessionExpired:
		return "session_expired"
	case KindAccountSuspended:
		return "account_suspended"
	case KindAccountLocked:
		return "account_locked"
	case KindStaleQuery:
		return "stale_query"
	case KindMissingFeatures:
		return "missing_features"
	case KindTargetNotFound:
		return "target_not_found"
	case KindTargetProtected:
		return "target_protected"
	case KindTargetSuspended:
		return "target_suspended"
	case KindBadRequest:
		return "bad_request"
	case KindForbidden:
		return "forbidden"
	}
	return "unknown"
}

// AccountFault reports whether the pool account (not the target, not our
// config) is the reason for the failure.
func (k Kind) AccountFault() bool {
	switch k {
	case KindRateLimited, KindSessionExpired, KindAccountSuspended, KindAccountLocked, KindForbidden:
		return true
	}
	return false
}

// Error is returned by every Client call. It never contains cookie values:
// Msg is X's own error message and Body is a truncated response body.
type Error struct {
	Kind    Kind
	Op      string
	Status  int
	Code    int       // X error code when present (88, 32, 64, ...)
	Msg     string    // X's message
	RetryAt time.Time // for RateLimited: when x-rate-limit-reset says we may retry
	Missing []string  // for MissingFeatures
	Err     error     // wrapped transport error, if any
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "x %s: %s", e.Op, e.Kind)
	if e.Status != 0 {
		fmt.Fprintf(&b, " (http %d", e.Status)
		if e.Code != 0 {
			fmt.Fprintf(&b, ", code %d", e.Code)
		}
		b.WriteString(")")
	}
	if e.Msg != "" {
		fmt.Fprintf(&b, ": %s", e.Msg)
	}
	if e.Err != nil {
		fmt.Fprintf(&b, ": %v", e.Err)
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf extracts the Kind from any error (KindUnknown if it is not ours).
func KindOf(err error) Kind {
	var xe *Error
	if errors.As(err, &xe) {
		return xe.Kind
	}
	return KindUnknown
}

// Known X error codes. https://developer.x.com/en/support/x-api/error-troubleshooting
const (
	codeCouldNotAuth     = 32
	codeSuspended        = 64
	codeRateLimit        = 88
	codeInvalidToken     = 89
	codeBadAuthData      = 215
	codeBadGuestToken    = 239
	codeLocked           = 326
	codeCsrfMismatch     = 353
	codeFeaturesNotNull  = 336
	codeUserNotFound     = 50
	codeUserSuspendedTgt = 63
)

var missingFeaturesRe = regexp.MustCompile(`(?i)features cannot be null:\s*(.+)$`)

// classify turns a non-success response into an *Error. headers is a
// lower-cased header lookup. Returns nil when the response is usable.
func classify(op string, status int, headers func(string) string, body []byte) *Error {
	res := gjson.ParseBytes(body)
	code, msg := firstXError(res)
	e := &Error{Op: op, Status: status, Code: code, Msg: truncate(msg, 300)}

	switch {
	case status == 429 || code == codeRateLimit:
		e.Kind = KindRateLimited
		e.RetryAt = parseReset(headers("x-rate-limit-reset"))
		return e
	case code == codeSuspended:
		e.Kind = KindAccountSuspended
		return e
	case code == codeLocked:
		e.Kind = KindAccountLocked
		return e
	case status == 401 || code == codeCouldNotAuth || code == codeInvalidToken ||
		code == codeBadAuthData || code == codeBadGuestToken || code == codeCsrfMismatch:
		e.Kind = KindSessionExpired
		return e
	case status >= 300 && status < 400:
		loc := strings.ToLower(headers("location"))
		switch {
		case strings.Contains(loc, "/account/access"):
			e.Kind = KindAccountLocked
		case strings.Contains(loc, "login") || strings.Contains(loc, "logout"):
			e.Kind = KindSessionExpired
		default:
			e.Kind = KindTransient
		}
		e.Msg = "redirect to " + loc
		return e
	case code == codeFeaturesNotNull || missingFeaturesRe.MatchString(msg):
		e.Kind = KindMissingFeatures
		e.Missing = parseMissingFeatures(msg)
		return e
	case status == 404:
		// GraphQL answers an unknown/rotated query id with a bare 404.
		e.Kind = KindStaleQuery
		if e.Msg == "" {
			e.Msg = "operation not found; query id is probably stale"
		}
		return e
	case status == 403:
		e.Kind = KindForbidden
		return e
	case status >= 500:
		e.Kind = KindTransient
		return e
	case status == 400:
		if strings.Contains(strings.ToLower(msg), "query") && strings.Contains(strings.ToLower(msg), "unspecified") {
			e.Kind = KindStaleQuery
		} else {
			e.Kind = KindBadRequest
		}
		return e
	case status != 200:
		e.Kind = KindUnknown
		return e
	}

	// HTTP 200. GraphQL can still carry errors with no data.
	if code != 0 && !res.Get("data").Exists() {
		switch code {
		case codeUserNotFound:
			e.Kind = KindTargetNotFound
		case codeUserSuspendedTgt:
			e.Kind = KindTargetSuspended
		default:
			e.Kind = KindBadRequest
		}
		return e
	}
	if !res.IsObject() {
		e.Kind = KindTransient
		e.Msg = "non-JSON 200 response"
		return e
	}
	return nil
}

func firstXError(res gjson.Result) (int, string) {
	errs := res.Get("errors")
	if !errs.IsArray() || len(errs.Array()) == 0 {
		return 0, ""
	}
	first := errs.Array()[0]
	code := int(first.Get("code").Int())
	if code == 0 {
		code = int(first.Get("extensions.code").Int())
	}
	return code, first.Get("message").String()
}

func parseReset(v string) time.Time {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	return time.Unix(n, 0)
}

func parseMissingFeatures(msg string) []string {
	m := missingFeaturesRe.FindStringSubmatch(msg)
	if len(m) < 2 {
		return nil
	}
	var out []string
	for _, f := range strings.Split(m[1], ",") {
		f = strings.Trim(strings.TrimSpace(f), `."'`)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
