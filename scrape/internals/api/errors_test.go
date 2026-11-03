package twitterAPI

import (
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func hdr(m map[string]string) headerGetter { return func(k string) string { return m[k] } }

func TestClassify(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	reset := strconv.FormatInt(now.Add(7*time.Minute).Unix(), 10)
	cases := []struct {
		name   string
		status int
		h      map[string]string
		body   string
		kind   Kind
	}{
		{"429 rate limit", 429, map[string]string{"x-rate-limit-reset": reset}, `{"errors":[{"code":88,"message":"Rate limit exceeded"}]}`, KindRateLimited},
		{"code 88 on 200", 200, nil, `{"errors":[{"code":88,"message":"Rate limit exceeded"}]}`, KindRateLimited},
		{"expired cookie", 401, nil, `{"errors":[{"code":32,"message":"Could not authenticate you"}]}`, KindSessionDead},
		{"bare 403", 403, nil, `{}`, KindSessionDead},
		{"suspended us", 403, nil, `{"errors":[{"code":64,"message":"Your account is suspended and is not permitted to access this feature."}]}`, KindSuspended},
		{"locked us", 403, nil, `{"errors":[{"code":326,"message":"Authorization: Denied by access control"}]}`, KindLocked},
		{"target missing", 200, nil, `{"errors":[{"code":50,"message":"User not found."}]}`, KindTargetGone},
		{"stale id is not 'target gone'", 404, nil, `{"errors":[{"message":"Not Found"}]}`, KindStaleRequest},
		{"missing features", 400, nil, `{"errors":[{"message":"The following features cannot be null: a_flag, b_flag"}]}`, KindStaleRequest},
		{"server error", 503, nil, `bad gateway`, KindTransient},
	}
	for _, c := range cases {
		e := classify(c.status, hdr(c.h), gjson.Parse(c.body), now)
		if e.Kind != c.kind {
			t.Errorf("%s: got %s want %s", c.name, e.Kind, c.kind)
		}
	}
	e := classify(429, hdr(map[string]string{"x-rate-limit-reset": reset}), gjson.Parse(`{}`), now)
	if e.RetryAfter != 7*time.Minute {
		t.Errorf("retry after = %s, want 7m", e.RetryAfter)
	}
	e = classify(400, nil, gjson.Parse(`{"errors":[{"message":"The following features cannot be null: a_flag, b_flag"}]}`), now)
	if len(e.MissingFeatures) != 2 || e.MissingFeatures[0] != "a_flag" || e.MissingFeatures[1] != "b_flag" {
		t.Errorf("missing features = %v", e.MissingFeatures)
	}
}

func TestErrorTextHasNoSecrets(t *testing.T) {
	e := classify(401, nil, gjson.Parse(`{"errors":[{"code":32,"message":"Could not authenticate you"}]}`), time.Now())
	if got := e.Error(); got == "" || len(got) > 300 {
		t.Fatalf("unexpected error text %q", got)
	}
}
