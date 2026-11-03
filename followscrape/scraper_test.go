package followscrape

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/jeetdotso/tweet-indexer-service/followscrape/mockx"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
)

// ---- in-memory pool and store -------------------------------------------------

type memAcct struct {
	name string
	cli  Client
}

func (a *memAcct) Name() string   { return a.name }
func (a *memAcct) Client() Client { return a.cli }

type memPool struct {
	mu       sync.Mutex
	srvURL   string
	tokens   []string
	inUse    map[string]bool
	rest     map[string]time.Time
	retired  map[string]int
	now      func() time.Time
	acquires map[string]int
}

func newMemPool(url string, now func() time.Time, tokens ...string) *memPool {
	return &memPool{srvURL: url, tokens: tokens, inUse: map[string]bool{}, rest: map[string]time.Time{},
		retired: map[string]int{}, now: now, acquires: map[string]int{}}
}

func (p *memPool) Acquire(ctx context.Context) (Account, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.tokens {
		if _, dead := p.retired[t]; dead || p.inUse[t] || p.rest[t].After(p.now()) {
			continue
		}
		cli, err := twitterAPI.NewTwitterClient(&twitterAPI.NewTwitterClientOptions{
			BaseURL: p.srvURL,
			Cookies: []*fhttp.Cookie{{Name: "auth_token", Value: t}, {Name: "ct0", Value: "csrf-" + t}},
		})
		if err != nil {
			return nil, err
		}
		p.inUse[t] = true
		p.acquires[t]++
		return &memAcct{name: t, cli: cli}, nil
	}
	return nil, ErrNoAccount
}
func (p *memPool) Release(_ context.Context, a Account) {
	p.mu.Lock()
	p.inUse[a.Name()] = false
	p.mu.Unlock()
}
func (p *memPool) Cooldown(_ context.Context, a Account, until time.Time, _ string) {
	p.mu.Lock()
	p.inUse[a.Name()] = false
	p.rest[a.Name()] = until
	p.mu.Unlock()
}
func (p *memPool) Retire(_ context.Context, a Account, group int, _ string) {
	p.mu.Lock()
	p.inUse[a.Name()] = false
	p.retired[a.Name()] = group
	p.mu.Unlock()
}

type memStore struct {
	mu     sync.Mutex
	rows   map[string]*model.Following
	writes int
}

func newMemStore() *memStore { return &memStore{rows: map[string]*model.Following{}} }
func (m *memStore) UpsertEdges(_ context.Context, edges []*model.Following) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range edges {
		m.rows[e.ID] = e
		m.writes++
	}
	return nil
}
func (m *memStore) CountByUserID(_ context.Context, uid string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, r := range m.rows {
		if r.UserID == uid {
			n++
		}
	}
	return n, nil
}
func (m *memStore) DeleteStaleForUser(_ context.Context, uid string, cutoff time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for id, r := range m.rows {
		if r.UserID == uid && r.CapturedAt.Before(cutoff) {
			delete(m.rows, id)
			n++
		}
	}
	return n, nil
}

// ---- helpers ------------------------------------------------------------------

func setOps() {
	twitterAPI.SetOperation("userByScreenName", twitterAPI.Operation{Name: "UserByScreenName", QueryID: "qid-user", Features: map[string]bool{"f": true}})
	twitterAPI.SetOperation("following", twitterAPI.Operation{Name: "Following", QueryID: "qid-following", Features: map[string]bool{"f": true}})
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

// fastOpts: no real sleeping; a fake clock advances by whatever the scraper would have slept.
func fastOpts(c *clock) Options {
	return Options{
		PageSize: 100, MaxAttemptsPerPage: 6, NoAccountTries: 2, Now: c.now,
		Sleep: func(_ context.Context, d time.Duration) { c.t = c.t.Add(d) },
	}
}

func profile(handle, id string, n int) mockx.Profile {
	return mockx.Profile{Handle: handle, ID: id, Name: handle, Following: n}
}

// ---- tests --------------------------------------------------------------------

func TestPagesToTheEndAndRerunDoesNotDuplicate(t *testing.T) {
	setOps()
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("big", "1", 1234)}, ValidTokens: []string{"a"}})
	defer srv.Close()
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	pool, store := newMemPool(srv.URL, c.now, "a"), newMemStore()
	sc := New(pool, store, fastOpts(c))

	res, err := sc.Scrape(context.Background(), "@big", "")
	if err != nil || !res.Complete {
		t.Fatalf("first run: err=%v complete=%v", err, res.Complete)
	}
	if res.Scraped != 1234 || res.StoredTotal != 1234 || res.Pages != 13 {
		t.Fatalf("scraped=%d stored=%d pages=%d, want 1234/1234/13", res.Scraped, res.StoredTotal, res.Pages)
	}
	// every row carries the requested fields
	r := store.rows["1_"+mockx.FollowedID("1", 10)]
	if r == nil || r.FollowingHandle == "" || r.FollowingName == "" || r.FollowingFollowersCount == 0 || r.CapturedAt.IsZero() || r.UserID != "1" || r.FollowingID != mockx.FollowedID("1", 10) {
		t.Fatalf("row missing fields: %+v", r)
	}

	c.t = c.t.Add(time.Hour)
	res2, err := sc.Scrape(context.Background(), "big", "")
	if err != nil || res2.StoredTotal != 1234 || len(store.rows) != 1234 {
		t.Fatalf("rerun: err=%v stored=%d rows=%d, want 1234", err, res2.StoredTotal, len(store.rows))
	}
	if !store.rows["1_"+mockx.FollowedID("1", 10)].CapturedAt.After(r.CapturedAt.Add(-time.Second)) {
		t.Fatal("rerun did not refresh captured_at")
	}
}

func TestLongRateLimitSwapsAccountsAndLosesNothing(t *testing.T) {
	setOps()
	// each account may make 2 Following calls per 30s window; 500 users = 5 pages
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "3", 500)}, ValidTokens: []string{"a", "b", "c"}, LimitPerWindow: 2, WindowSeconds: 3600})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a", "b", "c"), newMemStore()
	o := fastOpts(c)
	o.RateLimitMaxWait = time.Second // the 1 hour reset is far longer: must swap, not sleep
	sc := New(pool, store, o)

	res, err := sc.Scrape(context.Background(), "p", "")
	if err != nil || !res.Complete || res.Scraped != 500 || len(store.rows) != 500 {
		t.Fatalf("err=%v complete=%v scraped=%d rows=%d", err, res.Complete, res.Scraped, len(store.rows))
	}
	if res.RateLimited == 0 || res.AccountsSwaped == 0 {
		t.Fatalf("expected rate limit handling, got limited=%d swapped=%d", res.RateLimited, res.AccountsSwaped)
	}
	// limited accounts were rested, not retired or hammered
	if len(pool.retired) != 0 {
		t.Fatalf("rate limited accounts must not be retired: %v", pool.retired)
	}
	for tok, n := range srv.CallsSnapshot() {
		if n > 3 { // 2 allowed + at most the one call that got the 429
			t.Fatalf("account %s made %d Following calls; it should have been rested after its limit", tok, n)
		}
	}
}

func TestDeadSessionIsRetiredAndWorkContinuesWithoutGaps(t *testing.T) {
	setOps()
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "4", 450)}, ValidTokens: []string{"a", "b"}, DieAfter: map[string]int{"a": 2}})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a", "b"), newMemStore()
	res, err := New(pool, store, fastOpts(c)).Scrape(context.Background(), "p", "")
	if err != nil || !res.Complete || len(store.rows) != 450 {
		t.Fatalf("err=%v complete=%v rows=%d", err, res.Complete, len(store.rows))
	}
	if _, retired := pool.retired["a"]; !retired || len(pool.retired) != 1 {
		t.Fatalf("expected only account a retired, got %v", pool.retired)
	}
	for i := 1; i <= 450; i++ { // no gap, no skipped page
		if store.rows["4_"+mockx.FollowedID("4", i)] == nil {
			t.Fatalf("missing followed account #%d", i)
		}
	}
}

func TestTransientErrorsAreRetriedWithBackoff(t *testing.T) {
	setOps()
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "5", 300)}, ValidTokens: []string{"a"}, FailEveryNth: 2})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a"), newMemStore()
	res, err := New(pool, store, fastOpts(c)).Scrape(context.Background(), "p", "")
	if err != nil || !res.Complete || len(store.rows) != 300 {
		t.Fatalf("err=%v rows=%d", err, len(store.rows))
	}
	if len(pool.retired) != 0 {
		t.Fatal("a 503 must not retire an account")
	}
}

func TestStaleQueryIDStopsTheRunWithoutTouchingTheAccounts(t *testing.T) {
	setOps()
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "6", 50)}, ValidTokens: []string{"a", "b"}, AcceptedQueryIDs: []string{"some-other-id"}})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a", "b"), newMemStore()
	_, err := New(pool, store, fastOpts(c)).Scrape(context.Background(), "p", "")
	if !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("want ErrStaleRequest, got %v", err)
	}
	if len(pool.retired) != 0 || len(pool.rest) != 0 {
		t.Fatalf("a stale query id must not burn accounts: retired=%v rest=%v", pool.retired, pool.rest)
	}
	if pool.acquires["b"] != 0 {
		t.Fatal("must not try more accounts for a stale request")
	}
}

func TestMissingProtectedAndSuspendedTargetsAreReportedPerHandle(t *testing.T) {
	setOps()
	priv := profile("secret", "7", 10)
	priv.Protected = true
	gone := profile("banned", "8", 10)
	gone.Suspended = true
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{priv, gone, profile("ok", "9", 30)}, ValidTokens: []string{"a"}})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a"), newMemStore()
	sc := New(pool, store, fastOpts(c))
	for _, h := range []string{"nobody_here", "secret", "banned"} {
		_, err := sc.Scrape(context.Background(), h, "")
		ae, ok := twitterAPI.AsAPIError(err)
		if !ok || (ae.Kind != twitterAPI.KindTargetGone && ae.Kind != twitterAPI.KindTargetBlocked) {
			t.Fatalf("%s: want a target error, got %v", h, err)
		}
	}
	if len(pool.retired) != 0 || len(store.rows) != 0 {
		t.Fatal("bad targets must not retire accounts or store rows")
	}
	if res, err := sc.Scrape(context.Background(), "ok", ""); err != nil || res.Scraped != 30 {
		t.Fatalf("the run must continue with the next handle: %v %+v", err, res)
	}
}

func TestPoolExhaustedKeepsWhatWasReadAndResumes(t *testing.T) {
	setOps()
	srv := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "10", 500)}, ValidTokens: []string{"a"}, DieAfter: map[string]int{"a": 3}})
	defer srv.Close()
	c := &clock{t: time.Now()}
	pool, store := newMemPool(srv.URL, c.now, "a"), newMemStore()
	sc := New(pool, store, fastOpts(c))
	res, err := sc.Scrape(context.Background(), "p", "")
	if err == nil || res.Complete {
		t.Fatalf("expected a partial run, got complete=%v err=%v", res.Complete, err)
	}
	if res.ResumeCursor == "" || len(store.rows) == 0 || len(store.rows) >= 500 {
		t.Fatalf("partial run should keep progress: cursor=%q rows=%d", res.ResumeCursor, len(store.rows))
	}
	// a new, healthy account appears; resume from the cursor
	srv2 := mockx.New(mockx.Config{Profiles: []mockx.Profile{profile("p", "10", 500)}, ValidTokens: []string{"z"}})
	defer srv2.Close()
	pool2 := newMemPool(srv2.URL, c.now, "z")
	res2, err := New(pool2, store, fastOpts(c)).Scrape(context.Background(), "p", res.ResumeCursor)
	if err != nil || !res2.Complete || len(store.rows) != 500 {
		t.Fatalf("resume: err=%v complete=%v rows=%d", err, res2.Complete, len(store.rows))
	}
}

func TestNormalizeHandle(t *testing.T) {
	for in, want := range map[string]string{"jack": "jack", "@jack": "jack", " @Jack ": "Jack", "x.com/jack": "jack",
		"https://x.com/jack/": "jack", "https://twitter.com/jack/following": "jack"} {
		if got := NormalizeHandle(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}
