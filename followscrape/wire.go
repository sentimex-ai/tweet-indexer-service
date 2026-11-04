package followscrape

import (
	"context"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/indexer/repository"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"github.com/jeetdotso/tweet-indexer-service/scrape/xclient"
	"github.com/rs/zerolog"
)

// Deps is everything the scraper needs, built from config.
type Deps struct {
	Pool  *dummyaccount.ScrapePool
	Ops   *xclient.Ops
	Edges repository.FollowingRepository
	Runs  *MongoRuns
	XCfg  config.X
	Log   zerolog.Logger
}

func NewDeps(ctx context.Context, cfg *config.Config, conn *mongodb.MongoConnection, log zerolog.Logger) (*Deps, error) {
	ops, err := xclient.NewOps(cfg.X.Ops)
	if err != nil {
		return nil, err
	}
	sc := cfg.X.Scrape
	d := &Deps{
		Pool:  dummyaccount.NewScrapePool(conn, sc.AccountGroup, time.Duration(sc.LeaseTimeoutS)*time.Second),
		Ops:   ops,
		Edges: repository.NewFollowingRepository(conn),
		Runs:  NewMongoRuns(conn),
		XCfg:  cfg.X,
		Log:   log,
	}
	if err := d.Edges.EnsureIndexes(ctx); err != nil {
		return nil, err
	}
	if err := d.Runs.EnsureIndexes(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

// ClientFor builds an X client from a pool account's stored cookies.
func (d *Deps) ClientFor(acct *dummyaccount.SelAccount, opts ...xclient.Option) (*xclient.Client, error) {
	opts = append([]xclient.Option{xclient.WithWarner(func(msg string, kv ...any) {
		d.Log.Warn().Fields(kv).Msg(msg)
	})}, opts...)
	return xclient.New(d.XCfg, d.Ops, xclient.Session{Cookies: acct.GetCookiesHttp()}, "", opts...)
}

// Options maps config to scraper options. Overrides from the CLI are
// applied by the caller afterwards.
func (d *Deps) Options() Options {
	sc := d.XCfg.Scrape
	ms := func(v int) time.Duration { return time.Duration(v) * time.Millisecond }
	return Options{
		PageSize:           sc.PageSize,
		PageDelay:          ms(sc.PageDelayMs),
		MaxAttemptsPerPage: sc.MaxAttemptsPerPage,
		MaxAccountSwaps:    sc.MaxAccountSwaps,
		RateLimitCooldown:  time.Duration(sc.RateLimitCooldownS) * time.Second,
		BackoffBase:        ms(sc.BackoffBaseMs),
		BackoffMax:         ms(sc.BackoffMaxMs),
		MaxPoolWait:        20 * time.Minute,
		Prune:              true,
	}
}

func (d *Deps) NewScraper(opts Options) *Scraper {
	return New(d.Pool, func(a *dummyaccount.SelAccount) (API, error) { return d.ClientFor(a) }, d.Edges, d.Runs, opts, d.Log)
}
