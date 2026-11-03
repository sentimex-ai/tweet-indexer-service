// Package followscrape reads the full list of accounts an X profile follows and
// stores it. It is the on-demand replacement for the old tweet/Grok driven
// pipeline: give it handles, it pages each list to the end.
//
// It owns the failure policy. A rate limited account rests and another takes
// over, a dead session is retired from the pool, a missing or protected target
// is reported for that handle only, and a request X no longer understands stops
// the whole run before it burns the pool.
package followscrape

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
	following "github.com/jeetdotso/tweet-indexer-service/scrape/internals/parsers/following"
	"github.com/tidwall/gjson"
)

// Client is the part of the X client the scraper needs.
type Client interface {
	FullUser(handle string) (*gjson.Result, error)
	FollowingPage(userID, cursor string, count int) (*gjson.Result, error)
}

// Account is one logged in session borrowed from the pool.
type Account interface {
	Name() string
	Client() Client
}

// Pool hands out sessions and takes the verdict back. Implementations must be
// safe for concurrent use.
type Pool interface {
	// Acquire returns a free session, or ErrNoAccount if none is usable right now.
	Acquire(ctx context.Context) (Account, error)
	Release(ctx context.Context, a Account)
	// Cooldown rests a rate limited account until the given time.
	Cooldown(ctx context.Context, a Account, until time.Time, reason string)
	// Retire takes a dead, suspended or locked account out of rotation.
	Retire(ctx context.Context, a Account, group int, reason string)
}

// Store is where edges go.
type Store interface {
	UpsertEdges(ctx context.Context, edges []*model.Following) error
	CountByUserID(ctx context.Context, userID string) (int64, error)
	DeleteStaleForUser(ctx context.Context, userID string, cutoff time.Time) (int64, error)
}

var (
	// ErrNoAccount means the pool has nothing usable right now.
	ErrNoAccount = errors.New("no usable account in the pool")
	// ErrStaleRequest means X rejected our query id or feature flags. Retrying
	// with more accounts will not help; update x.operations in config.
	ErrStaleRequest = errors.New("x rejected the request format (query id or feature flags out of date)")
)

// Options tunes a Scraper. Zero values take the defaults below.
type Options struct {
	PageSize           int
	MaxPages           int // 0 = until the end
	MaxAttemptsPerPage int
	PageDelay          time.Duration
	RateLimitMaxWait   time.Duration // sleep up to this on a 429 before swapping account
	Cooldown           time.Duration // rest for a rate limited account
	TransientBackoff   time.Duration // first wait after a 5xx / timeout; doubles per retry
	NoAccountWait      time.Duration // wait before asking the pool again when it is empty
	NoAccountTries     int
	Prune              bool // after a complete run, delete edges not seen this run
	Now                func() time.Time
	Sleep              func(ctx context.Context, d time.Duration) // tests replace this
	Logf               func(format string, args ...interface{})
}

func (o *Options) fill() {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	defD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&o.PageSize, 100)
	def(&o.MaxAttemptsPerPage, 6)
	def(&o.NoAccountTries, 5)
	defD(&o.RateLimitMaxWait, 90*time.Second)
	defD(&o.Cooldown, 16*time.Minute)
	defD(&o.TransientBackoff, 2*time.Second)
	defD(&o.NoAccountWait, 30*time.Second)
	if o.PageDelay < 0 {
		o.PageDelay = 0
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = sleep
	}
	if o.Logf == nil {
		o.Logf = func(string, ...interface{}) {}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

type Scraper struct {
	pool  Pool
	store Store
	opt   Options
}

func New(pool Pool, store Store, opt Options) *Scraper {
	opt.fill()
	return &Scraper{pool: pool, store: store, opt: opt}
}

// Result describes one handle's run.
type Result struct {
	Handle         string    `json:"handle"`
	UserID         string    `json:"user_id"`
	ExpectedCount  int       `json:"expected_count"` // what the profile says it follows (approximate)
	Scraped        int       `json:"scraped"`        // distinct accounts stored this run
	Pages          int       `json:"pages"`
	StoredTotal    int64     `json:"stored_total"` // rows in following.indexes for this user afterwards
	Pruned         int64     `json:"pruned"`
	Complete       bool      `json:"complete"` // reached the end of the list
	ResumeCursor   string    `json:"resume_cursor,omitempty"`
	RateLimited    int       `json:"rate_limited"`
	AccountsSwaped int       `json:"accounts_swapped"`
	Started        time.Time `json:"started"`
	Finished       time.Time `json:"finished"`
	Error          string    `json:"error,omitempty"`
}

// NormalizeHandle accepts "jack", "@jack", "x.com/jack" or a full profile URL.
func NormalizeHandle(in string) string {
	h := strings.TrimSpace(in)
	if strings.Contains(h, "/") {
		if !strings.Contains(h, "://") {
			h = "https://" + h
		}
		if u, err := url.Parse(h); err == nil {
			h = strings.Trim(u.Path, "/")
			if i := strings.Index(h, "/"); i >= 0 {
				h = h[:i]
			}
		}
	}
	return strings.TrimPrefix(h, "@")
}

// Scrape reads handle's whole following list. startCursor resumes a run that
// stopped early (Result.ResumeCursor).
func (s *Scraper) Scrape(ctx context.Context, handle, startCursor string) (*Result, error) {
	handle = NormalizeHandle(handle)
	res := &Result{Handle: handle, Started: s.opt.Now(), ResumeCursor: startCursor}
	if handle == "" {
		return res, fail(res, s, errors.New("empty handle"))
	}

	sess := &session{s: s, res: res}
	defer sess.close(ctx)

	// 1) handle -> user id (and the profile's own counts)
	var user *gjson.Result
	if err := sess.do(ctx, func(c Client) error {
		r, err := c.FullUser(handle)
		if err != nil {
			return err
		}
		user = r
		return nil
	}); err != nil {
		return res, fail(res, s, fmt.Errorf("look up %q: %w", handle, err))
	}
	node := user.Get("data.user.result")
	switch {
	case !node.Exists():
		return res, fail(res, s, fmt.Errorf("look up %q: %w", handle, &twitterAPI.APIError{Kind: twitterAPI.KindTargetGone, Message: "no such user"}))
	case node.Get("__typename").String() == "UserUnavailable":
		return res, fail(res, s, fmt.Errorf("look up %q: %w", handle, &twitterAPI.APIError{Kind: twitterAPI.KindTargetBlocked, Message: "user unavailable: " + node.Get("reason").String()}))
	}
	res.UserID = node.Get("rest_id").String()
	res.ExpectedCount = int(firstExisting(node.Get("legacy.friends_count"), node.Get("relationship_counts.following")).Int())
	if res.UserID == "" {
		return res, fail(res, s, fmt.Errorf("look up %q: answer had no user id", handle))
	}
	protected := firstExisting(node.Get("legacy.protected"), node.Get("privacy.protected")).Bool()
	followedByUs := firstExisting(node.Get("relationship_perspectives.following"), node.Get("legacy.following")).Bool()
	if protected && !followedByUs {
		return res, fail(res, s, fmt.Errorf("%q is a protected profile that no pooled account follows: %w", handle,
			&twitterAPI.APIError{Kind: twitterAPI.KindTargetBlocked, Message: "protected"}))
	}

	// 2) page to the end
	seen := map[string]struct{}{}
	cursor := startCursor
	for {
		if err := ctx.Err(); err != nil {
			return res, fail(res, s, err)
		}
		if s.opt.MaxPages > 0 && res.Pages >= s.opt.MaxPages {
			res.ResumeCursor = cursor
			return res, fail(res, s, fmt.Errorf("stopped at max pages (%d)", s.opt.MaxPages))
		}
		var page *gjson.Result
		if err := sess.do(ctx, func(c Client) error {
			r, err := c.FollowingPage(res.UserID, cursor, s.opt.PageSize)
			if err != nil {
				return err
			}
			page = r
			return nil
		}); err != nil {
			res.ResumeCursor = cursor
			return res, fail(res, s, fmt.Errorf("page %d: %w", res.Pages+1, err))
		}

		entries, next := following.ExtractPage(page)
		res.Pages++
		now := s.opt.Now()
		edges := make([]*model.Following, 0, len(entries))
		for i := range entries {
			u := following.ParseFollowing(&entries[i])
			if u == nil || u.ID == "" || u.Handle == "" {
				s.opt.Logf("skipping unreadable entry on page %d of %s", res.Pages, handle)
				continue
			}
			if _, dup := seen[u.ID]; dup {
				continue
			}
			seen[u.ID] = struct{}{}
			edges = append(edges, model.NewFollowingDetailed(res.UserID, u.ID, u.Handle, u.UserName, u.Followers, u.Following, now))
		}
		if err := s.store.UpsertEdges(ctx, edges); err != nil {
			res.ResumeCursor = cursor
			return res, fail(res, s, fmt.Errorf("store page %d: %w", res.Pages, err))
		}
		res.Scraped = len(seen)
		s.opt.Logf("%s: page %d, +%d (total %d of about %d)", handle, res.Pages, len(edges), res.Scraped, res.ExpectedCount)

		if following.IsEnd(next) || len(entries) == 0 || next == cursor {
			res.Complete = true
			res.ResumeCursor = ""
			break
		}
		cursor = next
		res.ResumeCursor = cursor
		s.opt.Sleep(ctx, s.opt.PageDelay)
	}

	res.StoredTotal, _ = s.store.CountByUserID(ctx, res.UserID)
	if s.opt.Prune && startCursor == "" {
		res.Pruned, _ = s.store.DeleteStaleForUser(ctx, res.UserID, res.Started)
		res.StoredTotal, _ = s.store.CountByUserID(ctx, res.UserID)
	}
	res.Finished = s.opt.Now()
	return res, nil
}

func fail(res *Result, s *Scraper, err error) error {
	res.Error = err.Error()
	res.Finished = s.opt.Now()
	return err
}

func firstExisting(vals ...gjson.Result) gjson.Result {
	for _, v := range vals {
		if v.Exists() {
			return v
		}
	}
	return gjson.Result{}
}

// session is the account currently in use by one Scrape call.
type session struct {
	s   *Scraper
	res *Result
	acc Account
}

func (se *session) close(ctx context.Context) {
	if se.acc != nil {
		se.s.pool.Release(context.WithoutCancel(ctx), se.acc)
		se.acc = nil
	}
}

func (se *session) ensure(ctx context.Context) error {
	if se.acc != nil {
		return nil
	}
	var last error
	for i := 0; i < se.s.opt.NoAccountTries; i++ {
		a, err := se.s.pool.Acquire(ctx)
		if err == nil && a != nil {
			se.acc = a
			return nil
		}
		last = err
		if err != nil && !errors.Is(err, ErrNoAccount) {
			return err
		}
		se.s.opt.Logf("no usable account (try %d/%d), waiting %s", i+1, se.s.opt.NoAccountTries, se.s.opt.NoAccountWait)
		se.s.opt.Sleep(ctx, se.s.opt.NoAccountWait)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if last == nil {
		last = ErrNoAccount
	}
	return fmt.Errorf("pool exhausted: %w", last)
}

// do runs fn with a live account and applies the failure policy until fn
// succeeds or the policy says stop.
func (se *session) do(ctx context.Context, fn func(Client) error) error {
	o := se.s.opt
	backoff := o.TransientBackoff
	var lastErr error
	for attempt := 1; attempt <= o.MaxAttemptsPerPage; attempt++ {
		if err := se.ensure(ctx); err != nil {
			return err
		}
		err := fn(se.acc.Client())
		if err == nil {
			return nil
		}
		lastErr = err
		ae, ok := twitterAPI.AsAPIError(err)
		if !ok {
			// Not an X answer (decode problem, bug). Do not hammer X over it.
			return err
		}
		switch ae.Kind {
		case twitterAPI.KindRateLimited:
			se.res.RateLimited++
			wait := ae.RetryAfter
			if wait > 0 && wait <= o.RateLimitMaxWait {
				// Short window: waiting here is cheaper than burning another account.
				o.Logf("rate limited, waiting %s for the window to reopen", wait.Round(time.Second))
				o.Sleep(ctx, wait+time.Second)
				continue
			}
			rest := o.Cooldown
			if wait > rest {
				rest = wait
			}
			o.Logf("%s rate limited, resting until %s and swapping", se.acc.Name(), o.Now().Add(rest).Format(time.RFC3339))
			se.s.pool.Cooldown(ctx, se.acc, o.Now().Add(rest), "rate limited")
			se.acc = nil
			se.res.AccountsSwaped++
		case twitterAPI.KindSessionDead:
			o.Logf("%s session is dead, retiring it", se.acc.Name())
			se.s.pool.Retire(ctx, se.acc, dummyaccount.NOT_AUTHENTICATED_GROUP, ae.Error())
			se.acc = nil
			se.res.AccountsSwaped++
		case twitterAPI.KindSuspended:
			o.Logf("%s is suspended, retiring it", se.acc.Name())
			se.s.pool.Retire(ctx, se.acc, dummyaccount.SUSPENDED_GROUP, ae.Error())
			se.acc = nil
			se.res.AccountsSwaped++
		case twitterAPI.KindLocked:
			o.Logf("%s is locked, retiring it", se.acc.Name())
			se.s.pool.Retire(ctx, se.acc, dummyaccount.NOT_WORKING_GROUP, ae.Error())
			se.acc = nil
			se.res.AccountsSwaped++
		case twitterAPI.KindStaleRequest:
			return fmt.Errorf("%w: %s", ErrStaleRequest, ae.Error())
		case twitterAPI.KindTargetGone, twitterAPI.KindTargetBlocked:
			return err
		default: // transient or unknown: back off, same account
			o.Logf("transient failure (%s), retry in %s", ae.Error(), backoff)
			o.Sleep(ctx, backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("gave up after %d attempts: %w", o.MaxAttemptsPerPage, lastErr)
}
