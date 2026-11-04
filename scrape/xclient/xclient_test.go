package xclient_test

import (
	"context"
	"strings"
	"testing"

	http "github.com/bogdanfinn/fhttp"
	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/internal/fakex"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/tidwall/gjson"
)

const secretToken = "SECRET-AUTH-TOKEN-VALUE"
const secretCT0 = "SECRET-CT0-VALUE"

func newClient(t *testing.T, fx *fakex.Server, token string) *xclient.Client {
	t.Helper()
	ops, err := xclient.NewOps(map[string]config.XOp{
		// lower-case key on purpose: viper lower-cases map keys
		"userbyscreenname": {QueryID: fx.UserQueryID, Features: `{"a":true}`, FieldToggles: `{"withAuxiliaryUserLabels":false}`},
		"following":        {QueryID: fx.FollowingQueryID, Features: `{"b":true}`, ExtraVariables: `{"includePromotedContent":false}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := xclient.New(config.X{BaseURL: fx.URL, Bearer: "pub", UserAgent: "ua"}, ops,
		xclient.Session{Cookies: []*http.Cookie{{Name: "auth_token", Value: token}, {Name: "ct0", Value: secretCT0}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestUserAndFollowingAgainstFake(t *testing.T) {
	fx := fakex.New()
	defer fx.Close()
	fx.Add(fakex.Profile{ID: "42", Handle: "Alice", Following: 120})
	c := newClient(t, fx, secretToken)
	ctx := context.Background()

	u, ri, err := c.UserByScreenName(ctx, "@alice")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "42" || u.Handle != "Alice" || u.FollowingCount != 120 {
		t.Fatalf("bad user %+v", u)
	}
	if ri.Limit != 500 || ri.Remaining != 499 {
		t.Fatalf("rate info %+v", ri)
	}
	if fx.SeenCSRF[0] != secretCT0 {
		t.Fatal("x-csrf-token must equal ct0")
	}

	total, pages, cursor := 0, 0, ""
	for {
		p, _, err := c.FollowingPage(ctx, u.ID, u.Handle, cursor, 50)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		total += len(p.Users) + p.Skipped
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if total != 120 || pages != 3 {
		t.Fatalf("got %d entries over %d pages", total, pages)
	}
}

func TestErrorKindsAndNoCookieLeak(t *testing.T) {
	fx := fakex.New()
	defer fx.Close()
	fx.Add(fakex.Profile{ID: "7", Handle: "bob", Following: 10})
	fx.Add(fakex.Profile{ID: "8", Handle: "gone", Suspended: true})
	ctx := context.Background()

	cases := []struct {
		name  string
		setup func()
		token string
		call  func(c *xclient.Client) error
		want  xclient.Kind
	}{
		{"expired", func() { fx.Expired["dead"] = true }, "dead", func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "bob"); return e }, xclient.KindSessionExpired},
		{"suspended account", func() { fx.Suspended["susp"] = true }, "susp", func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "bob"); return e }, xclient.KindAccountSuspended},
		{"rate limited", func() { fx.RateLimitAfter["rl"] = 0 }, "rl", func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "bob"); return e }, xclient.KindRateLimited},
		{"503", func() { fx.FailNext503 = 1 }, secretToken, func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "bob"); return e }, xclient.KindTransient},
		{"unknown handle", nil, secretToken, func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "nobody"); return e }, xclient.KindTargetNotFound},
		{"suspended target", nil, secretToken, func(c *xclient.Client) error { _, _, e := c.UserByScreenName(ctx, "gone"); return e }, xclient.KindTargetSuspended},
		{"stale query id", func() { fx.FollowingQueryID = "rotated" }, secretToken, func(c *xclient.Client) error { _, _, e := c.FollowingPage(ctx, "7", "bob", "", 20); return e }, xclient.KindStaleQuery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, fx, tc.token) // built from config before X "rotates"
			if tc.setup != nil {
				tc.setup()
			}
			err := tc.call(c)
			if xclient.KindOf(err) != tc.want {
				t.Fatalf("kind = %v (%v), want %v", xclient.KindOf(err), err, tc.want)
			}
			if strings.Contains(err.Error(), secretToken) || strings.Contains(err.Error(), secretCT0) {
				t.Fatalf("error leaks a cookie: %v", err)
			}
		})
	}
}

func TestRateLimitResetParsed(t *testing.T) {
	fx := fakex.New()
	defer fx.Close()
	fx.Add(fakex.Profile{ID: "7", Handle: "bob", Following: 10})
	fx.RateLimitAfter["rl"] = 0
	_, _, err := newClient(t, fx, "rl").UserByScreenName(context.Background(), "bob")
	var xe *xclient.Error
	if !asErr(err, &xe) || xe.RetryAt.IsZero() {
		t.Fatalf("want RetryAt from x-rate-limit-reset, got %v", err)
	}
}

func TestMissingFeatureAutoAdded(t *testing.T) {
	fx := fakex.New()
	defer fx.Close()
	fx.Add(fakex.Profile{ID: "7", Handle: "bob", Following: 10})
	fx.RequiredFeature = "brand_new_flag_enabled"
	c := newClient(t, fx, secretToken)
	p, _, err := c.FollowingPage(context.Background(), "7", "bob", "", 20)
	if err != nil {
		t.Fatalf("expected auto-heal, got %v", err)
	}
	if len(p.Users) != 10 {
		t.Fatalf("got %d users", len(p.Users))
	}
}

func asErr(err error, target **xclient.Error) bool {
	xe, ok := err.(*xclient.Error)
	if ok {
		*target = xe
	}
	return ok
}

// Real payload shapes: 2025 (core.*) and pre-2025 (legacy.*).
func TestParseUserBothLayouts(t *testing.T) {
	newer := gjson.Parse(`{"__typename":"User","rest_id":"1","is_blue_verified":true,
		"core":{"name":"New Name","screen_name":"newh"},"avatar":{"image_url":"https://a"},
		"privacy":{"protected":false},"legacy":{"followers_count":5,"friends_count":6}}`)
	older := gjson.Parse(`{"__typename":"User","rest_id":"2",
		"legacy":{"name":"Old Name","screen_name":"oldh","followers_count":7,"friends_count":8,"profile_image_url_https":"https://b","protected":true}}`)
	u, ok := xclient.ParseUser(newer)
	if !ok || u.Handle != "newh" || u.DisplayName != "New Name" || u.FollowerCount != 5 || u.FollowingCount != 6 || !u.Verified {
		t.Fatalf("newer layout: %+v", u)
	}
	u, ok = xclient.ParseUser(older)
	if !ok || u.Handle != "oldh" || u.DisplayName != "Old Name" || !u.Protected || u.ProfileImg != "https://b" {
		t.Fatalf("older layout: %+v", u)
	}
	if _, ok := xclient.ParseUser(gjson.Parse(`{"__typename":"UserUnavailable","reason":"Suspended"}`)); ok {
		t.Fatal("unavailable user must not parse")
	}
}

func TestParseFollowingReplaceEntryCursor(t *testing.T) {
	res := gjson.Parse(`{"data":{"user":{"result":{"timeline":{"timeline":{"instructions":[
		{"type":"TimelineAddEntries","entries":[
			{"entryId":"user-9","content":{"itemContent":{"user_results":{"result":{"__typename":"User","rest_id":"9","core":{"screen_name":"n","name":"N"},"legacy":{}}}}}}]},
		{"type":"TimelineReplaceEntry","entry_id_to_replace":"cursor-bottom-1","entry":{"entryId":"cursor-bottom-1","content":{"value":"5|6"}}}
	]}}}}}}`)
	p, err := xclient.ParseFollowingPage(res)
	if err != nil || len(p.Users) != 1 || p.NextCursor != "5|6" {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestNormalizeHandle(t *testing.T) {
	for in, want := range map[string]string{
		"@elonmusk": "elonmusk", " x ": "x", "https://x.com/jack/following": "jack",
		"twitter.com/naval?s=20": "naval",
	} {
		if got := xclient.NormalizeHandle(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}
