// Package followscrape scrapes the complete following list of public X
// profiles and stores it in following.indexes.
//
// Failure policy, by xclient.Kind:
//
//	rate_limited       park the account until x-rate-limit-reset, continue on another
//	session_expired    move account to NOT_AUTHENTICATED_GROUP, continue on another
//	account_suspended  move account to SUSPENDED_GROUP, continue on another
//	account_locked     move account to NOT_WORKING_GROUP, continue on another
//	forbidden          short cool-down (could be systemic, so never retire on it)
//	transient          exponential backoff with jitter on the same account
//	target_*           stop this handle, no retry, no account touched
//	stale_query, ...   stop: config problem, rotating accounts cannot help
//
// A circuit breaker stops the whole batch when several *different* accounts
// fail the same way in a row without any success in between: that pattern
// means X changed something (bearer, transaction-id requirement, IP block)
// and continuing would retire the entire pool for nothing.
package followscrape

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/rs/zerolog"
	uuid "github.com/satori/go.uuid"
)

// API is the slice of xclient.Client the scraper uses.
type API interface {
	UserByScreenName(ctx context.Context, handle string) (xclient.User, xclient.RateInfo, error)
	FollowingPage(ctx context.Context, userID, handle, cursor string, count int) (xclient.Page, xclient.RateInfo, error)
}

type ClientFactory func(acct *dummyaccount.SelAccount) (API, error)

type Pool interface {
	Acquire(ctx context.Context) (*dummyaccount.SelAccount, error)
	Release(ctx context.Context, username string) error
	CoolDown(ctx context.Context, username string, until time.Time, reason string) error
	Retire(ctx context.Context, username string, group int, reason string) error
	NextAvailable(ctx context.Context) (time.Time, bool, error)
}

type EdgeStore interface {
	UpsertEdges(ctx context.Context, edges []*model.Following) (int64, int64, error)
	PruneNotSeen(ctx context.Context, userID, runID string) (int64, error)
	CountByUserID(ctx context.Context, userID string) (int64, error)
}

type RunStore interface {
	Save(ctx context.Context, r *Run) error
	LatestResumable(ctx context.Context, handle string) (*Run, error)
}

type Options struct {
	PageSize           int
	PageDelay          time.Duration
	MaxAttemptsPerPage int           // transient retries per request
	MaxAccountSwaps    int           // account rotations per handle
	RateLimitCooldown  time.Duration // when X sends no reset header
	ForbiddenCooldown  time.Duration
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	MaxPoolWait        time.Duration // longest we wait for a cooling account
	BreakerThreshold   int           // distinct accounts failing identically in a row
	Prune              bool          // delete edges not seen by a complete run
	Resume             bool          // continue the latest partial run of a handle
	MaxPages           int           // safety cap; 0 = none
}

func (o *Options) defaults() {
	if o.PageSize <= 0 {
		o.PageSize = 50
	}
	if o.MaxAttemptsPerPage <= 0 {
		o.MaxAttemptsPerPage = 6
	}
	if o.MaxAccountSwaps <= 0 {
		o.MaxAccountSwaps = 5
	}
	if o.RateLimitCooldown <= 0 {
		o.RateLimitCooldown = 15 * time.Minute
	}
	if o.ForbiddenCooldown <= 0 {
		o.ForbiddenCooldown = 10 * time.Minute
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = 2 * time.Second
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = time.Minute
	}
	if o.MaxPoolWait < 0 {
		o.MaxPoolWait = 0
	}
	if o.BreakerThreshold <= 0 {
		o.BreakerThreshold = 3
	}
}

// Run statuses.
const (
	StatusRunning   = "running"
	StatusComplete  = "complete"
	StatusPartial   = "partial" // stopped early; saved edges are kept, nothing pruned
	StatusFailed    = "failed"
	StatusNotFound  = "not_found"
	StatusProtected = "protected"
	StatusSuspended = "suspended"
)

// Run is one scrape of one handle; persisted to following.runs.
type Run struct {
	ID          string    `bson:"_id" json:"id"`
	Handle      string    `bson:"handle" json:"handle"`
	UserID      string    `bson:"user_id" json:"user_id"`
	Status      string    `bson:"status" json:"status"`
	Expected    int       `bson:"expected" json:"expected"` // friends_count from the profile
	Scraped     int       `bson:"scraped" json:"scraped"`   // edges returned by X this run
	Stored      int64     `bson:"stored" json:"stored"`     // edges in Mongo for this user after the run
	Inserted    int64     `bson:"inserted" json:"inserted"`
	Updated     int64     `bson:"updated" json:"updated"`
	Pruned      int64     `bson:"pruned" json:"pruned"`
	Skipped     int       `bson:"skipped" json:"skipped"` // unavailable users in the list
	Pages       int       `bson:"pages" json:"pages"`
	Accounts    int       `bson:"accounts_used" json:"accounts_used"`
	LastCursor  string    `bson:"last_cursor" json:"-"`
	ErrorKind   string    `bson:"error_kind" json:"error_kind,omitempty"`
	Error       string    `bson:"error" json:"error,omitempty"`
	StartedAt   time.Time `bson:"started_at" json:"started_at"`
	FinishedAt  time.Time `bson:"finished_at" json:"finished_at"`
	DurationSec float64   `bson:"duration_sec" json:"duration_sec"`
}

var (
	ErrPoolExhausted = errors.New("account pool exhausted")
	ErrCircuitOpen   = errors.New("circuit breaker open")
)

type Scraper struct {
	pool    Pool
	newAPI  ClientFactory
	edges   EdgeStore
	runs    RunStore
	opts    Options
	log     zerolog.Logger
	sleep   func(ctx context.Context, d time.Duration) error
	now     func() time.Time
	breaker *breaker
}

func New(pool Pool, newAPI ClientFactory, edges EdgeStore, runs RunStore, opts Options, log zerolog.Logger) *Scraper {
	opts.defaults()
	return &Scraper{
		pool: pool, newAPI: newAPI, edges: edges, runs: runs, opts: opts, log: log,
		sleep: sleepCtx, now: time.Now,
		breaker: &breaker{threshold: opts.BreakerThreshold},
	}
}

// ScrapeAll scrapes handles sequentially. Sequential is deliberate: the
// limit is per-account rate limits, not CPU, and one-at-a-time keeps the
// pool healthy. It stops early only if the circuit breaker opens.
func (s *Scraper) ScrapeAll(ctx context.Context, handles []string) []*Run {
	var out []*Run
	for _, h := range handles {
		if ctx.Err() != nil {
			break
		}
		r, err := s.Scrape(ctx, h)
		out = append(out, r)
		if errors.Is(err, ErrCircuitOpen) {
			s.log.Error().Err(err).Msg("stopping batch: circuit breaker open")
			break
		}
	}
	return out
}

// Scrape fetches the full following list of one handle.
func (s *Scraper) Scrape(ctx context.Context, rawHandle string) (*Run, error) {
	handle := xclient.NormalizeHandle(rawHandle)
	run := &Run{ID: uuid.NewV4().String(), Handle: strings.ToLower(handle), Status: StatusRunning, StartedAt: s.now()}
	if s.opts.Resume {
		if prev, err := s.runs.LatestResumable(ctx, run.Handle); err == nil && prev != nil {
			run = prev
			run.Status = StatusRunning
			run.Error, run.ErrorKind = "", ""
			s.log.Info().Str("handle", handle).Str("run_id", run.ID).Int("already_scraped", run.Scraped).Msg("resuming partial run")
		}
	}
	log := s.log.With().Str("handle", handle).Str("run_id", run.ID).Logger()
	_ = s.runs.Save(ctx, run)

	sess := &session{s: s, log: log, run: run}
	defer sess.close(context.WithoutCancel(ctx))

	err := s.scrape(ctx, sess, handle, run, log)
	s.finish(ctx, run, err, log)
	return run, err
}

func (s *Scraper) scrape(ctx context.Context, sess *session, handle string, run *Run, log zerolog.Logger) error {
	// 1) handle -> user (always refresh: counts/protected may have changed)
	var user xclient.User
	if err := sess.do(ctx, func(api API) error {
		u, _, err := api.UserByScreenName(ctx, handle)
		user = u
		return err
	}); err != nil {
		return err
	}
	if run.UserID != "" && run.UserID != user.ID {
		// handle now belongs to a different account; do not mix lists
		run.LastCursor, run.Scraped, run.Pages = "", 0, 0
		run.ID = uuid.NewV4().String()
	}
	run.UserID, run.Expected = user.ID, user.FollowingCount
	log.Info().Str("user_id", user.ID).Int("following_count", user.FollowingCount).Bool("protected", user.Protected).Msg("resolved handle")
	if user.Protected {
		return &xclient.Error{Kind: xclient.KindTargetProtected, Op: xclient.OpUserByScreenName, Msg: handle + " is protected"}
	}

	// 2) page through Following
	cursor := run.LastCursor
	seen := map[string]bool{}
	emptyPages := 0
	for {
		if s.opts.MaxPages > 0 && run.Pages >= s.opts.MaxPages {
			return fmt.Errorf("max pages (%d) reached", s.opts.MaxPages)
		}
		var page xclient.Page
		var ri xclient.RateInfo
		if err := sess.do(ctx, func(api API) error {
			p, r, err := api.FollowingPage(ctx, user.ID, handle, cursor, s.opts.PageSize)
			page, ri = p, r
			return err
		}); err != nil {
			return err
		}
		run.Pages++
		run.Skipped += page.Skipped

		if len(page.Users) > 0 {
			emptyPages = 0
			now := s.now()
			edges := make([]*model.Following, 0, len(page.Users))
			for _, u := range page.Users {
				edges = append(edges, &model.Following{
					ID:            model.FollowingID(user.ID, u.ID),
					UserID:        user.ID,
					FollowingID:   u.ID,
					SourceHandle:  strings.ToLower(user.Handle),
					Handle:        u.Handle,
					DisplayName:   u.DisplayName,
					FollowerCount: u.FollowerCount, FollowingCount: u.FollowingCount,
					Verified: u.Verified, Protected: u.Protected, ProfileImg: u.ProfileImg,
					CapturedAt: now, FirstSeenAt: now, RunID: run.ID, UpdatedAt: now,
				})
			}
			ins, upd, err := s.edges.UpsertEdges(ctx, edges)
			if err != nil {
				return fmt.Errorf("store page %d: %w", run.Pages, err)
			}
			run.Scraped += len(page.Users)
			run.Inserted += ins
			run.Updated += upd
		} else {
			emptyPages++
		}

		log.Info().Int("page", run.Pages).Int("users", len(page.Users)).Int("total", run.Scraped).
			Int("expected", run.Expected).Int("rate_remaining", ri.Remaining).Msg("page stored")

		next := page.NextCursor
		switch {
		case next == "":
			return nil // end of list
		case emptyPages >= 2:
			log.Info().Msg("two empty pages in a row, treating as end of list")
			return nil
		case seen[next]:
			log.Warn().Msg("cursor repeated, treating as end of list")
			return nil
		}
		seen[next] = true
		cursor = next
		run.LastCursor = next
		_ = s.runs.Save(ctx, run) // checkpoint so --resume can continue

		// Respect the account's own window: if this was the last call it
		// allows, park it now instead of eating a 429.
		if ri.Remaining == 0 && !ri.Reset.IsZero() {
			sess.parkCurrent(ctx, ri.Reset, "window exhausted")
		}
		if err := s.sleep(ctx, s.opts.PageDelay); err != nil {
			return err
		}
	}
}

func (s *Scraper) finish(ctx context.Context, run *Run, err error, log zerolog.Logger) {
	ctx = context.WithoutCancel(ctx)
	run.FinishedAt = s.now()
	run.DurationSec = run.FinishedAt.Sub(run.StartedAt).Seconds()
	if err == nil {
		run.Status = StatusComplete
		run.LastCursor = ""
		if s.opts.Prune && run.UserID != "" {
			n, perr := s.edges.PruneNotSeen(ctx, run.UserID, run.ID)
			if perr != nil {
				log.Error().Err(perr).Msg("prune failed")
			}
			run.Pruned = n
		}
	} else {
		run.ErrorKind = xclient.KindOf(err).String()
		run.Error = err.Error()
		switch xclient.KindOf(err) {
		case xclient.KindTargetNotFound:
			run.Status = StatusNotFound
		case xclient.KindTargetProtected:
			run.Status = StatusProtected
		case xclient.KindTargetSuspended:
			run.Status = StatusSuspended
		default:
			if run.Scraped > 0 {
				run.Status = StatusPartial
			} else {
				run.Status = StatusFailed
			}
		}
	}
	if run.UserID != "" {
		run.Stored, _ = s.edges.CountByUserID(ctx, run.UserID)
	}
	_ = s.runs.Save(ctx, run)

	ev := log.Info()
	if run.Status != StatusComplete {
		ev = log.Warn().Str("error_kind", run.ErrorKind).Str("error", run.Error)
	}
	ev.Str("status", run.Status).Int("scraped", run.Scraped).Int("expected", run.Expected).
		Int64("stored", run.Stored).Int64("inserted", run.Inserted).Int64("updated", run.Updated).
		Int64("pruned", run.Pruned).Int("skipped_unavailable", run.Skipped).Int("pages", run.Pages).
		Int("accounts_used", run.Accounts).Float64("duration_sec", run.DurationSec).Msg("scrape finished")
}

// ---------------------------------------------------------------------------
// account session: lease/rotate/park accounts around each request

type session struct {
	s     *Scraper
	log   zerolog.Logger
	run   *Run
	acct  *dummyaccount.SelAccount
	api   API
	swaps int
}

func (ss *session) close(ctx context.Context) {
	if ss.acct != nil {
		if err := ss.s.pool.Release(ctx, ss.acct.UserName); err != nil {
			ss.log.Error().Err(err).Msg("release account failed")
		}
		ss.acct, ss.api = nil, nil
	}
}

func (ss *session) parkCurrent(ctx context.Context, until time.Time, reason string) {
	if ss.acct == nil {
		return
	}
	ss.log.Info().Str("account", Mask(ss.acct.UserName)).Time("until", until).Str("reason", reason).Msg("parking account")
	_ = ss.s.pool.CoolDown(context.WithoutCancel(ctx), ss.acct.UserName, until, reason)
	ss.acct, ss.api = nil, nil
}

func (ss *session) ensure(ctx context.Context) error {
	for ss.api == nil {
		if ss.swaps >= ss.s.opts.MaxAccountSwaps {
			return fmt.Errorf("%w: used %d accounts for this handle", ErrPoolExhausted, ss.swaps)
		}
		acct, err := ss.s.pool.Acquire(ctx)
		if errors.Is(err, dummyaccount.ErrNoAccounts) {
			until, ok, nerr := ss.s.pool.NextAvailable(ctx)
			if nerr != nil {
				return nerr
			}
			wait := time.Until(until)
			if !ok || wait > ss.s.opts.MaxPoolWait {
				if ok {
					return fmt.Errorf("%w: next account frees up at %s (in %s, max wait %s)", ErrPoolExhausted,
						until.Format(time.RFC3339), wait.Round(time.Second), ss.s.opts.MaxPoolWait)
				}
				return fmt.Errorf("%w: no accounts with cookies in the working group", ErrPoolExhausted)
			}
			ss.log.Info().Dur("wait", wait.Round(time.Second)).Msg("all accounts cooling down, waiting")
			if err := ss.s.sleep(ctx, wait+time.Second); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		api, err := ss.s.newAPI(acct)
		if err != nil {
			// unusable stored session (missing auth_token/ct0)
			ss.log.Warn().Str("account", Mask(acct.UserName)).Err(err).Msg("account has no usable session, retiring")
			_ = ss.s.pool.Retire(ctx, acct.UserName, dummyaccount.NOT_AUTHENTICATED_GROUP, xclient.KindSessionExpired.String())
			ss.swaps++
			continue
		}
		ss.acct, ss.api = acct, api
		ss.swaps++
		ss.run.Accounts++
		ss.log.Debug().Str("account", Mask(acct.UserName)).Msg("leased account")
	}
	return nil
}

// do runs fn with a healthy account, rotating/parking accounts and backing
// off according to the failure kind.
func (ss *session) do(ctx context.Context, fn func(API) error) error {
	transient := 0
	for {
		if err := ss.s.breaker.check(); err != nil {
			return err
		}
		if err := ss.ensure(ctx); err != nil {
			return err
		}
		err := fn(ss.api)
		if err == nil {
			ss.s.breaker.success()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		kind := xclient.KindOf(err)
		var xe *xclient.Error
		errors.As(err, &xe)
		acct := ss.acct.UserName
		alog := ss.log.Warn().Str("account", Mask(acct)).Str("kind", kind.String()).Err(err)

		if kind.AccountFault() {
			ss.s.breaker.fail(kind, acct)
		}
		switch kind {
		case xclient.KindRateLimited:
			until := ss.s.now().Add(ss.s.opts.RateLimitCooldown)
			if xe != nil && !xe.RetryAt.IsZero() && xe.RetryAt.After(ss.s.now()) {
				until = xe.RetryAt.Add(5 * time.Second)
			}
			alog.Time("until", until).Msg("rate limited, parking account and rotating")
			_ = ss.s.pool.CoolDown(ctx, acct, until, kind.String())
		case xclient.KindForbidden:
			until := ss.s.now().Add(ss.s.opts.ForbiddenCooldown)
			alog.Time("until", until).Msg("403 without error code, cooling account (not retiring)")
			_ = ss.s.pool.CoolDown(ctx, acct, until, kind.String())
		case xclient.KindSessionExpired:
			alog.Msg("session expired, moving account to NOT_AUTHENTICATED group")
			_ = ss.s.pool.Retire(ctx, acct, dummyaccount.NOT_AUTHENTICATED_GROUP, kind.String())
		case xclient.KindAccountSuspended:
			alog.Msg("account suspended, moving to SUSPENDED group")
			_ = ss.s.pool.Retire(ctx, acct, dummyaccount.SUSPENDED_GROUP, kind.String())
		case xclient.KindAccountLocked:
			alog.Msg("account locked, moving to NOT_WORKING group")
			_ = ss.s.pool.Retire(ctx, acct, dummyaccount.NOT_WORKING_GROUP, kind.String())
		case xclient.KindTransient, xclient.KindUnknown:
			transient++
			if transient >= ss.s.opts.MaxAttemptsPerPage {
				alog.Int("attempts", transient).Msg("giving up after transient errors")
				return err
			}
			d := backoff(ss.s.opts.BackoffBase, ss.s.opts.BackoffMax, transient)
			alog.Int("attempt", transient).Dur("backoff", d).Msg("transient error, backing off")
			if serr := ss.s.sleep(ctx, d); serr != nil {
				return serr
			}
			continue
		default:
			// target or config problem: no account is at fault
			return err
		}
		ss.acct, ss.api = nil, nil
	}
}

// ---------------------------------------------------------------------------

// breaker trips when `threshold` distinct accounts fail with the same
// account-fault kind consecutively (rate limits excluded: those are normal).
type breaker struct {
	mu        sync.Mutex
	threshold int
	kind      xclient.Kind
	accounts  map[string]bool
	open      error
}

func (b *breaker) check() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.kind, b.accounts = 0, nil
}

func (b *breaker) fail(k xclient.Kind, acct string) {
	if k == xclient.KindRateLimited {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if k != b.kind {
		b.kind, b.accounts = k, map[string]bool{}
	}
	b.accounts[acct] = true
	if len(b.accounts) >= b.threshold {
		b.open = fmt.Errorf("%w: %d different accounts failed with %s in a row; this looks systemic "+
			"(or every stored session really is expired) "+
			"(X changed bearer/headers/transaction-id requirements, or the IP is blocked). Stopping "+
			"so the rest of the pool is not retired. Check with `scrape-following check`, then restore "+
			"accounts with `scrape-following accounts restore`", ErrCircuitOpen, len(b.accounts), k)
	}
}

func backoff(base, max time.Duration, attempt int) time.Duration {
	d := base << (attempt - 1)
	if d > max || d <= 0 {
		d = max
	}
	// full jitter in [d/2, d)
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Mask shortens a pool account username for logs: "scraper_good" ->
// "sc…od#1a2b". The hash suffix keeps different accounts distinguishable;
// `scrape-following accounts` shows the full names.
func Mask(u string) string {
	sum := sha256.Sum256([]byte(u))
	tag := hex.EncodeToString(sum[:2])
	if len(u) <= 4 {
		return "…#" + tag
	}
	return u[:2] + "…" + u[len(u)-2:] + "#" + tag
}
