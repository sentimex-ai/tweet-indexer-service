package followscrape

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/internal/fakex"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/rs/zerolog"
	"github.com/tebeka/selenium"
)

// ---- in-memory fakes -------------------------------------------------------

type memAcct struct {
	name   string
	group  int
	inUse  bool
	until  time.Time
	reason string
	used   time.Time
}

type memPool struct {
	mu    sync.Mutex
	accts []*memAcct
}

func newPool(names ...string) *memPool {
	p := &memPool{}
	for i, n := range names {
		p.accts = append(p.accts, &memAcct{name: n, used: time.Unix(int64(i), 0)})
	}
	return p
}

func (p *memPool) get(n string) *memAcct {
	for _, a := range p.accts {
		if a.name == n {
			return a
		}
	}
	return nil
}

func (p *memPool) Acquire(ctx context.Context) (*dummyaccount.SelAccount, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sort.Slice(p.accts, func(i, j int) bool { return p.accts[i].used.Before(p.accts[j].used) })
	for _, a := range p.accts {
		if a.group == 0 && !a.inUse && !a.until.After(time.Now()) {
			a.inUse, a.used = true, time.Now()
			return &dummyaccount.SelAccount{UserName: a.name, Cookies: []selenium.Cookie{
				{Name: "auth_token", Value: a.name}, {Name: "ct0", Value: "csrf-" + a.name}}}, nil
		}
	}
	return nil, dummyaccount.ErrNoAccounts
}
func (p *memPool) Release(ctx context.Context, u string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.get(u).inUse = false
	return nil
}
func (p *memPool) CoolDown(ctx context.Context, u string, until time.Time, r string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	a := p.get(u)
	a.inUse, a.until, a.reason = false, until, r
	return nil
}
func (p *memPool) Retire(ctx context.Context, u string, g int, r string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	a := p.get(u)
	a.inUse, a.group, a.reason = false, g, r
	return nil
}
func (p *memPool) NextAvailable(ctx context.Context) (time.Time, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var best time.Time
	for _, a := range p.accts {
		if a.group == 0 && a.until.After(time.Now()) && (best.IsZero() || a.until.Before(best)) {
			best = a.until
		}
	}
	return best, !best.IsZero(), nil
}

type memEdges struct {
	mu sync.Mutex
	m  map[string]model.Following
}

func (e *memEdges) UpsertEdges(ctx context.Context, es []*model.Following) (int64, int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var ins, upd int64
	for _, x := range es {
		if old, ok := e.m[x.ID]; ok {
			x.FirstSeenAt = old.FirstSeenAt
			upd++
		} else {
			ins++
		}
		e.m[x.ID] = *x
	}
	return ins, upd, nil
}
func (e *memEdges) PruneNotSeen(ctx context.Context, uid, run string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int64
	for k, v := range e.m {
		if v.UserID == uid && v.RunID != run {
			delete(e.m, k)
			n++
		}
	}
	return n, nil
}
func (e *memEdges) CountByUserID(ctx context.Context, uid string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int64
	for _, v := range e.m {
		if v.UserID == uid {
			n++
		}
	}
	return n, nil
}

type memRuns struct {
	mu sync.Mutex
	m  map[string]Run
}

func (r *memRuns) Save(ctx context.Context, x *Run) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[x.ID] = *x
	return nil
}
func (r *memRuns) LatestResumable(ctx context.Context, h string) (*Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best *Run
	for _, v := range r.m {
		v := v
		if v.Handle == h && v.LastCursor != "" && v.Status != StatusComplete && (best == nil || v.StartedAt.After(best.StartedAt)) {
			best = &v
		}
	}
	return best, nil
}

// ---- harness -----------------------------------------------------------------

type harness struct {
	fx    *fakex.Server
	pool  *memPool
	edges *memEdges
	runs  *memRuns
	opts  Options
	slept time.Duration
}

func newHarness(t *testing.T, accounts ...string) *harness {
	fx := fakex.New()
	t.Cleanup(fx.Close)
	return &harness{
		fx: fx, pool: newPool(accounts...),
		edges: &memEdges{m: map[string]model.Following{}},
		runs:  &memRuns{m: map[string]Run{}},
		opts:  Options{PageSize: 50, Prune: true, MaxPoolWait: time.Minute, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond},
	}
}

func (h *harness) scraper(t *testing.T) *Scraper {
	ops, err := xclient.NewOps(map[string]config.XOp{
		"UserByScreenName": {QueryID: h.fx.UserQueryID},
		"Following":        {QueryID: h.fx.FollowingQueryID},
	})
	if err != nil {
		t.Fatal(err)
	}
	xc := config.X{BaseURL: h.fx.URL, Bearer: "pub", UserAgent: "ua"}
	factory := func(a *dummyaccount.SelAccount) (API, error) {
		return xclient.New(xc, ops, xclient.Session{Cookies: a.GetCookiesHttp()}, "")
	}
	s := New(h.pool, factory, h.edges, h.runs, h.opts, zerolog.New(io.Discard))
	// Never really sleep in unit tests; record the requested waits.
	s.sleep = func(ctx context.Context, d time.Duration) error {
		h.slept += d
		return ctx.Err()
	}
	return s
}

func unavailable(n int) int { return n / 97 } // fakex: every 97th entry is a suspended user

// ---- tests -------------------------------------------------------------------

func TestFullListThenRerunNoDuplicates(t *testing.T) {
	h := newHarness(t, "acc1")
	h.fx.Add(fakex.Profile{ID: "1000", Handle: "bigfollower", Following: 1234})
	ctx := context.Background()

	r, err := h.scraper(t).Scrape(ctx, "@BigFollower")
	if err != nil {
		t.Fatal(err)
	}
	want := int64(1234 - unavailable(1234))
	if r.Status != StatusComplete || r.Stored != want || r.Inserted != want || r.Pages != 25 || r.Skipped != unavailable(1234) {
		t.Fatalf("first run: %+v", r)
	}
	// fields
	e := h.edges.m[model.FollowingID("1000", fakex.FollowedID("1000", 0))]
	if e.Handle == "" || e.DisplayName == "" || e.SourceHandle != "bigfollower" || e.CapturedAt.IsZero() || e.FollowerCount == 0 {
		t.Fatalf("edge fields missing: %+v", e)
	}
	first := e.FirstSeenAt

	r2, err := h.scraper(t).Scrape(ctx, "bigfollower")
	if err != nil {
		t.Fatal(err)
	}
	if r2.Stored != want || r2.Inserted != 0 || r2.Updated != want || r2.Pruned != 0 {
		t.Fatalf("re-run must only update: %+v", r2)
	}
	if len(h.edges.m) != int(want) {
		t.Fatalf("duplicates: %d docs", len(h.edges.m))
	}
	e2 := h.edges.m[e.ID]
	if !e2.FirstSeenAt.Equal(first) || !e2.CapturedAt.After(e.CapturedAt) && !e2.CapturedAt.Equal(e.CapturedAt) {
		t.Fatalf("first_seen_at must be stable, captured_at refreshed: %v %v", e2.FirstSeenAt, first)
	}
	if h.pool.get("acc1").inUse {
		t.Fatal("account not released")
	}
}

func TestUnfollowsArePruned(t *testing.T) {
	h := newHarness(t, "acc1")
	h.fx.Add(fakex.Profile{ID: "5", Handle: "p", Following: 300})
	ctx := context.Background()
	if _, err := h.scraper(t).Scrape(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	h.fx.SetFollowing("p", 200) // unfollowed the last 100
	r, err := h.scraper(t).Scrape(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if r.Stored != int64(200-unavailable(200)) || r.Pruned != int64(100-(unavailable(300)-unavailable(200))) {
		t.Fatalf("%+v", r)
	}
}

func TestRateLimitMidListRotatesAndContinues(t *testing.T) {
	h := newHarness(t, "acc1", "acc2")
	h.fx.Add(fakex.Profile{ID: "9", Handle: "p", Following: 1000})
	h.fx.RateLimitAfter["acc1"] = 6 // 1 user lookup + 5 pages, then 429
	r, err := h.scraper(t).Scrape(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusComplete || r.Stored != int64(1000-unavailable(1000)) || r.Accounts != 2 {
		t.Fatalf("%+v", r)
	}
	a1 := h.pool.get("acc1")
	if a1.group != 0 || !a1.until.After(time.Now()) || a1.reason != "rate_limited" && a1.reason != "window exhausted" {
		t.Fatalf("acc1 should be cooling down, not retired: %+v", a1)
	}
}

func TestDeadSessionsAreRetiredNotRetried(t *testing.T) {
	h := newHarness(t, "expired", "susp", "good")
	h.fx.Add(fakex.Profile{ID: "9", Handle: "p", Following: 120})
	h.fx.Expired["expired"] = true
	h.fx.Suspended["susp"] = true
	r, err := h.scraper(t).Scrape(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusComplete {
		t.Fatalf("%+v", r)
	}
	if g := h.pool.get("expired").group; g != dummyaccount.NOT_AUTHENTICATED_GROUP {
		t.Fatalf("expired -> group %d", g)
	}
	if g := h.pool.get("susp").group; g != dummyaccount.SUSPENDED_GROUP {
		t.Fatalf("suspended -> group %d", g)
	}
	if g := h.pool.get("good").group; g != 0 {
		t.Fatal("good account touched")
	}
}

func TestPoolExhaustedIsPartialThenResumes(t *testing.T) {
	h := newHarness(t, "only")
	h.opts.MaxPoolWait = 0 // do not wait for the cooldown
	h.fx.Add(fakex.Profile{ID: "3", Handle: "p", Following: 500})
	h.fx.RateLimitAfter["only"] = 4
	ctx := context.Background()

	r, err := h.scraper(t).Scrape(ctx, "p")
	if !errors.Is(err, ErrPoolExhausted) || r.Status != StatusPartial || r.Scraped == 0 || r.LastCursor == "" {
		t.Fatalf("want partial with checkpoint, got %+v / %v", r, err)
	}
	if r.Pruned != 0 {
		t.Fatal("partial run must not prune")
	}

	// cooldown over, quota restored
	delete(h.fx.RateLimitAfter, "only")
	h.pool.get("only").until = time.Time{}
	h.opts.Resume = true
	r2, err := h.scraper(t).Scrape(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if r2.ID != r.ID || r2.Status != StatusComplete || r2.Stored != int64(500-unavailable(500)) {
		t.Fatalf("resume: %+v", r2)
	}
}

func TestCircuitBreakerStopsSystemicFailure(t *testing.T) {
	h := newHarness(t, "a", "b", "c", "d", "e")
	h.fx.Add(fakex.Profile{ID: "9", Handle: "p", Following: 10})
	h.fx.Add(fakex.Profile{ID: "10", Handle: "q", Following: 10})
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		h.fx.Expired[n] = true // e.g. X rotated the bearer: every account "expires"
	}
	runs := h.scraper(t).ScrapeAll(context.Background(), []string{"p", "q"})
	if len(runs) != 1 || runs[0].Status != StatusFailed || !strings.Contains(runs[0].Error, "circuit breaker open") {
		t.Fatalf("batch should stop after first handle: %+v", runs)
	}
	retired := 0
	for _, a := range h.pool.accts {
		if a.group != 0 {
			retired++
		}
	}
	if retired != 3 {
		t.Fatalf("breaker should stop after 3 accounts, %d retired", retired)
	}
}

func TestProtectedTargetTouchesNoAccount(t *testing.T) {
	h := newHarness(t, "acc1")
	h.fx.Add(fakex.Profile{ID: "9", Handle: "locked", Following: 50, Protected: true})
	r, err := h.scraper(t).Scrape(context.Background(), "locked")
	if xclient.KindOf(err) != xclient.KindTargetProtected || r.Status != StatusProtected {
		t.Fatalf("%+v %v", r, err)
	}
	if a := h.pool.get("acc1"); a.group != 0 || !a.until.IsZero() || a.inUse {
		t.Fatalf("account touched: %+v", a)
	}
}

func TestTransientErrorsBackOff(t *testing.T) {
	h := newHarness(t, "acc1")
	h.fx.Add(fakex.Profile{ID: "9", Handle: "p", Following: 60})
	h.fx.FailNext503 = 3
	r, err := h.scraper(t).Scrape(context.Background(), "p")
	if err != nil || r.Status != StatusComplete || r.Accounts != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if h.slept == 0 {
		t.Fatal("expected backoff sleeps")
	}
}

func TestNotFound(t *testing.T) {
	h := newHarness(t, "acc1")
	r, _ := h.scraper(t).Scrape(context.Background(), "doesnotexist")
	if r.Status != StatusNotFound {
		t.Fatalf("%+v", r)
	}
}
