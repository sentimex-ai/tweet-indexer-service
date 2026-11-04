// Package xclient is a small client for the two x.com web GraphQL operations
// the following scraper needs: UserByScreenName and Following.
//
// Everything that rotates with X web deploys (query ids, feature flags) comes
// from config.X, and every failure is returned as a typed *Error so callers
// can tell a rate limit from a dead session from a stale query id.
//
// Cookie values are only ever written into the outgoing Cookie header. They
// are never logged, never put in URLs and never included in errors.
package xclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	http "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"

	"github.com/jeetdotso/tweet-indexer-service/config"
)

const (
	OpUserByScreenName = "UserByScreenName"
	OpFollowing        = "Following"

	maxBodyBytes = 8 << 20
)

// Doer is the subset of tls_client.HttpClient we use (lets tests swap it).
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Ops holds the configured GraphQL operations. It is shared by every Client
// built from the same config so a feature auto-added for one account is used
// by all of them.
type Ops struct {
	mu  sync.RWMutex
	ops map[string]*op
}

type op struct {
	name         string
	queryID      string
	features     map[string]any
	fieldToggles map[string]any
	extraVars    map[string]any
}

// NewOps parses the JSON blobs in config. Op names are matched
// case-insensitively because viper lower-cases map keys.
func NewOps(cfg map[string]config.XOp) (*Ops, error) {
	o := &Ops{ops: map[string]*op{}}
	for _, name := range []string{OpUserByScreenName, OpFollowing} {
		var c config.XOp
		var found bool
		for k, v := range cfg {
			if strings.EqualFold(k, name) {
				c, found = v, true
				break
			}
		}
		if !found || c.QueryID == "" {
			return nil, fmt.Errorf("config x.ops.%s.queryId is required", name)
		}
		parsed := &op{name: name, queryID: strings.TrimSpace(c.QueryID)}
		var err error
		if parsed.features, err = parseJSONObj(c.Features); err != nil {
			return nil, fmt.Errorf("x.ops.%s.features: %w", name, err)
		}
		if parsed.fieldToggles, err = parseJSONObj(c.FieldToggles); err != nil {
			return nil, fmt.Errorf("x.ops.%s.fieldToggles: %w", name, err)
		}
		if parsed.extraVars, err = parseJSONObj(c.ExtraVariables); err != nil {
			return nil, fmt.Errorf("x.ops.%s.extraVariables: %w", name, err)
		}
		o.ops[name] = parsed
	}
	return o, nil
}

func parseJSONObj(s string) (map[string]any, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	m := map[string]any{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// QueryID returns the configured id of an operation.
func (o *Ops) QueryID(name string) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if p, ok := o.ops[name]; ok {
		return p.queryID
	}
	return ""
}

// addFeatures marks features X said were required. Returns false if nothing
// new was added (so we do not loop forever on the same error).
func (o *Ops) addFeatures(name string, feats []string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := o.ops[name]
	if p == nil {
		return false
	}
	if p.features == nil {
		p.features = map[string]any{}
	}
	added := false
	for _, f := range feats {
		if _, ok := p.features[f]; !ok {
			p.features[f] = false
			added = true
		}
	}
	return added
}

func (o *Ops) snapshot(name string) (op, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	p, ok := o.ops[name]
	if !ok {
		return op{}, false
	}
	return *p, true
}

// Session is the auth material for one logged-in X account.
type Session struct {
	// All cookies from the stored browser session; auth_token and ct0 are
	// the ones that matter.
	Cookies []*http.Cookie
}

func (s Session) cookie(name string) string {
	for _, c := range s.Cookies {
		if c != nil && c.Name == name {
			return c.Value
		}
	}
	return ""
}

// Valid reports whether the session has the two cookies X needs.
func (s Session) Valid() bool { return s.cookie("auth_token") != "" && s.cookie("ct0") != "" }

// Warner receives non-fatal operator-facing warnings (e.g. auto-added features).
type Warner func(msg string, kv ...any)

type Client struct {
	base    string
	bearer  string
	ua      string
	headers map[string]string
	ops     *Ops
	sess    Session
	http    Doer
	warn    Warner
}

type Option func(*Client)

func WithDoer(d Doer) Option     { return func(c *Client) { c.http = d } }
func WithWarner(w Warner) Option { return func(c *Client) { c.warn = w } }

// New builds a client for one session. proxyURL may be empty.
func New(cfg config.X, ops *Ops, sess Session, proxyURL string, opts ...Option) (*Client, error) {
	if !sess.Valid() {
		return nil, &Error{Kind: KindSessionExpired, Op: "init", Msg: "session is missing auth_token or ct0 cookie"}
	}
	c := &Client{
		base:    strings.TrimRight(firstNonEmpty(cfg.BaseURL, "https://x.com"), "/"),
		bearer:  cfg.Bearer,
		ua:      cfg.UserAgent,
		headers: cfg.Headers,
		ops:     ops,
		sess:    sess,
		warn:    func(string, ...any) {},
	}
	for _, o := range opts {
		o(c)
	}
	if c.http == nil {
		copts := []tls_client.HttpClientOption{
			tls_client.WithTimeoutSeconds(30),
			tls_client.WithClientProfile(profiles.Chrome_133_PSK),
			tls_client.WithNotFollowRedirects(),
		}
		if proxyURL != "" {
			copts = append(copts, tls_client.WithProxyUrl(proxyURL))
		}
		hc, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), copts...)
		if err != nil {
			return nil, fmt.Errorf("tls client: %w", err)
		}
		c.http = hc
	}
	return c, nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// RateInfo is X's rate-limit bookkeeping for the endpoint just called.
type RateInfo struct {
	Limit     int
	Remaining int // -1 when the header was absent
	Reset     time.Time
}

// graphQL performs one GET and returns the parsed body. It retries once,
// in-process, when X reports newly required feature flags.
func (c *Client) graphQL(ctx context.Context, name string, vars map[string]any, referer string) ([]byte, RateInfo, error) {
	for attempt := 0; attempt < 2; attempt++ {
		body, ri, err := c.graphQLOnce(ctx, name, vars, referer)
		if err == nil {
			return body, ri, nil
		}
		xe, ok := err.(*Error)
		if ok && xe.Kind == KindMissingFeatures && len(xe.Missing) > 0 && c.ops.addFeatures(name, xe.Missing) {
			c.warn("x requires new feature flags; added for this process, update config x.ops", "op", name, "features", strings.Join(xe.Missing, ","))
			continue
		}
		return nil, ri, err
	}
	return nil, RateInfo{}, &Error{Kind: KindMissingFeatures, Op: name, Msg: "feature flags still rejected after auto-fix"}
}

func (c *Client) graphQLOnce(ctx context.Context, name string, vars map[string]any, referer string) ([]byte, RateInfo, error) {
	p, ok := c.ops.snapshot(name)
	if !ok {
		return nil, RateInfo{}, &Error{Kind: KindStaleQuery, Op: name, Msg: "operation not configured"}
	}
	allVars := map[string]any{}
	for k, v := range p.extraVars {
		allVars[k] = v
	}
	for k, v := range vars {
		allVars[k] = v
	}
	q := url.Values{}
	q.Set("variables", mustJSON(allVars))
	if len(p.features) > 0 {
		q.Set("features", mustJSON(p.features))
	}
	if len(p.fieldToggles) > 0 {
		q.Set("fieldToggles", mustJSON(p.fieldToggles))
	}
	u := fmt.Sprintf("%s/i/api/graphql/%s/%s?%s", c.base, url.PathEscape(p.queryID), name, q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, RateInfo{}, &Error{Kind: KindBadRequest, Op: name, Err: err}
	}
	c.setHeaders(req, referer)

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, RateInfo{}, ctx.Err()
		}
		// Transport errors can embed the URL but never headers, so no cookies.
		return nil, RateInfo{}, &Error{Kind: KindTransient, Op: name, Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, RateInfo{}, &Error{Kind: KindTransient, Op: name, Status: resp.StatusCode, Err: err}
	}
	hdr := func(k string) string { return resp.Header.Get(k) }
	ri := parseRateInfo(hdr)
	if xe := classify(name, resp.StatusCode, hdr, body); xe != nil {
		return nil, ri, xe
	}
	return body, ri, nil
}

func (c *Client) setHeaders(req *http.Request, referer string) {
	ct0 := c.sess.cookie("ct0")
	h := http.Header{
		"accept":                    {"*/*"},
		"accept-language":           {"en-US,en;q=0.9"},
		"authorization":             {"Bearer " + c.bearer},
		"content-type":              {"application/json"},
		"referer":                   {referer},
		"sec-ch-ua":                 {`"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"`},
		"sec-ch-ua-mobile":          {"?0"},
		"sec-ch-ua-platform":        {`"Windows"`},
		"sec-fetch-dest":            {"empty"},
		"sec-fetch-mode":            {"cors"},
		"sec-fetch-site":            {"same-origin"},
		"user-agent":                {c.ua},
		"x-csrf-token":              {ct0},
		"x-twitter-active-user":     {"yes"},
		"x-twitter-auth-type":       {"OAuth2Session"},
		"x-twitter-client-language": {"en"},
	}
	for k, v := range c.headers {
		h.Set(k, v)
	}
	// Only name=value pairs; never attributes. Sorted for stable requests.
	pairs := make([]string, 0, len(c.sess.Cookies))
	for _, ck := range c.sess.Cookies {
		if ck != nil && ck.Name != "" {
			pairs = append(pairs, ck.Name+"="+ck.Value)
		}
	}
	sort.Strings(pairs)
	h.Set("cookie", strings.Join(pairs, "; "))
	h[http.HeaderOrderKey] = []string{
		"accept", "accept-language", "authorization", "content-type", "cookie", "referer",
		"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "sec-fetch-dest", "sec-fetch-mode",
		"sec-fetch-site", "user-agent", "x-csrf-token", "x-twitter-active-user",
		"x-twitter-auth-type", "x-twitter-client-language",
	}
	req.Header = h
}

func parseRateInfo(hdr func(string) string) RateInfo {
	ri := RateInfo{Remaining: -1}
	fmt.Sscanf(hdr("x-rate-limit-limit"), "%d", &ri.Limit)
	if v := hdr("x-rate-limit-remaining"); v != "" {
		fmt.Sscanf(v, "%d", &ri.Remaining)
	}
	ri.Reset = parseReset(hdr("x-rate-limit-reset"))
	return ri
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
