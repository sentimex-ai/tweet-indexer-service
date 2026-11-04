package xclient

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"

	http "github.com/bogdanfinn/fhttp"
)

// Discovered is what the live x.com bundle says about one operation.
type Discovered struct {
	Name            string
	QueryID         string
	FeatureSwitches []string // names only; X decides values server-side
	FieldToggles    []string
	Source          string // bundle file it was found in
}

var (
	scriptSrcRe = regexp.MustCompile(`https://abs\.twimg\.com/[A-Za-z0-9_\-./]+\.js`)
	importRe    = regexp.MustCompile(`["'(](?:\./|/)?((?:assets/)?[A-Za-z0-9_\-.]+\.js)["')]`)
	// {queryId:"abc",operationName:"Following",operationType:"query",metadata:{featureSwitches:[..],fieldToggles:[..]}}
	legacyOpRe = regexp.MustCompile(`queryId:"([A-Za-z0-9_\-]+)",operationName:"([A-Za-z0-9_]+)",operationType:"[a-z]+",metadata:\{featureSwitches:\[([^\]]*)\],fieldToggles:\[([^\]]*)\]`)
	// looser fallbacks for reordered/minified shapes
	looseOpRe1 = regexp.MustCompile(`queryId:\s*"([A-Za-z0-9_\-]{10,40})"\s*,\s*operationName:\s*"([A-Za-z0-9_]+)"`)
	looseOpRe2 = regexp.MustCompile(`operationName:\s*"([A-Za-z0-9_]+)"[^{}]{0,80}?queryId:\s*"([A-Za-z0-9_\-]{10,40})"`)
	quotedRe   = regexp.MustCompile(`"([A-Za-z0-9_]+)"`)
)

// Discover loads x.com as this session (logged-in pages ship the full
// GraphQL operation map; the logged-out shell does not) and scans its JS
// bundles for the wanted operations. Best effort: it depends on how X
// bundles its code, which is itself subject to change.
func (c *Client) Discover(ctx context.Context, wanted []string, maxFiles int) (map[string]Discovered, error) {
	if maxFiles <= 0 {
		maxFiles = 400
	}
	want := map[string]bool{}
	for _, w := range wanted {
		want[w] = true
	}
	html, status, err := c.getRaw(ctx, c.base+"/home", true)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, &Error{Kind: KindSessionExpired, Op: "discover", Status: status, Msg: "x.com/home did not load for this session"}
	}

	queue := uniq(scriptSrcRe.FindAllString(string(html), -1))
	seen := map[string]bool{}
	out := map[string]Discovered{}

	for len(queue) > 0 && len(seen) < maxFiles && len(out) < len(want) {
		u := queue[0]
		queue = queue[1:]
		if seen[u] {
			continue
		}
		seen[u] = true
		js, st, err := c.getRaw(ctx, u, false)
		if err != nil || st != 200 {
			continue
		}
		src := string(js)
		for _, d := range scanOps(src) {
			if want[d.Name] {
				if _, dup := out[d.Name]; !dup {
					d.Source = u
					out[d.Name] = d
				}
			}
		}
		// follow relative chunk imports
		base, _ := url.Parse(u)
		for _, m := range importRe.FindAllStringSubmatch(src, -1) {
			ref, err := url.Parse(m[1])
			if err != nil || base == nil {
				continue
			}
			abs := base.ResolveReference(ref).String()
			if strings.Contains(m[1], "assets/") {
				// Vite style: assets/ is relative to the bundle root
				root := u[:strings.LastIndex(u, "/")+1]
				if i := strings.Index(root, "/assets/"); i >= 0 {
					root = root[:i+1]
				}
				abs = root + m[1]
			}
			if !seen[abs] {
				queue = append(queue, abs)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("discover: scanned %d bundle files, found none of %v (bundle format changed?)", len(seen), wanted)
	}
	return out, nil
}

func scanOps(src string) []Discovered {
	var res []Discovered
	for _, m := range legacyOpRe.FindAllStringSubmatch(src, -1) {
		res = append(res, Discovered{Name: m[2], QueryID: m[1], FeatureSwitches: quoted(m[3]), FieldToggles: quoted(m[4])})
	}
	if len(res) > 0 {
		return res
	}
	for _, m := range looseOpRe1.FindAllStringSubmatch(src, -1) {
		res = append(res, Discovered{Name: m[2], QueryID: m[1]})
	}
	for _, m := range looseOpRe2.FindAllStringSubmatch(src, -1) {
		res = append(res, Discovered{Name: m[1], QueryID: m[2]})
	}
	return res
}

func quoted(s string) []string {
	var out []string
	for _, m := range quotedRe.FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}

func uniq(in []string) []string {
	m := map[string]bool{}
	var out []string
	for _, s := range in {
		if !m[s] {
			m[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// getRaw fetches a page; withSession attaches the account cookies (only for
// x.com itself, never for the CDN).
func (c *Client) getRaw(ctx context.Context, u string, withSession bool) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header = http.Header{
		"accept":          {"*/*"},
		"accept-language": {"en-US,en;q=0.9"},
		"user-agent":      {c.ua},
	}
	if withSession {
		pairs := []string{}
		for _, ck := range c.sess.Cookies {
			if ck != nil && ck.Name != "" {
				pairs = append(pairs, ck.Name+"="+ck.Value)
			}
		}
		req.Header.Set("cookie", strings.Join(pairs, "; "))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, &Error{Kind: KindTransient, Op: "discover", Err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	return b, resp.StatusCode, err
}
