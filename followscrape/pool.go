package followscrape

import (
	"context"
	"errors"
	"fmt"
	fhttp "github.com/bogdanfinn/fhttp"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/logg"
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
)

// MongoPool lends out the logged in accounts stored in dummy.accounts (the same
// pool the rest of the service uses). Only the account's name is ever logged.
type MongoPool struct {
	Svc      dummyaccount.Service
	BaseURL  string
	ProxyURL string
}

type mongoAccount struct {
	name   string
	client *twitterAPI.TwitterClient
}

func (a *mongoAccount) Name() string   { return a.name }
func (a *mongoAccount) Client() Client { return a.client }

func (p *MongoPool) Acquire(ctx context.Context) (Account, error) {
	for tries := 0; tries < 50; tries++ { // 50 = more than any sane number of unusable rows
		sel, err := p.Svc.LogInToNextGroup(ctx, dummyaccount.WORKING_GROUP)
		if err != nil {
			if errors.Is(err, dummyaccount.ErrNoAccount) {
				return nil, ErrNoAccount
			}
			return nil, err
		}
		cookies := sel.GetCookiesHttp()
		if !hasCookie(cookies, "ct0") || !hasCookie(cookies, "auth_token") {
			// A row without the two session cookies can never work. Park it.
			logg.Log.Warn().Str("account", sel.UserName).Msg("account has no session cookies, moving it out of the working group")
			_ = p.Svc.Retire(ctx, sel.UserName, dummyaccount.NOT_AUTHENTICATED_GROUP)
			continue
		}
		cli, err := twitterAPI.NewTwitterClient(&twitterAPI.NewTwitterClientOptions{
			BaseURL: p.BaseURL, Cookies: cookies, ProxyURL: p.ProxyURL,
		})
		if err != nil {
			_ = p.Svc.LogOut(ctx, sel.UserName)
			return nil, err
		}
		return &mongoAccount{name: sel.UserName, client: cli}, nil
	}
	return nil, fmt.Errorf("every account tried had no session cookies")
}

func hasCookie(cs []*fhttp.Cookie, name string) bool {
	for _, c := range cs {
		if c.Name == name && c.Value != "" {
			return true
		}
	}
	return false
}

func (p *MongoPool) Release(ctx context.Context, a Account) {
	if err := p.Svc.LogOut(ctx, a.Name()); err != nil {
		logg.Log.Error().Err(err).Str("account", a.Name()).Msg("could not release account")
	}
}

func (p *MongoPool) Cooldown(ctx context.Context, a Account, until time.Time, reason string) {
	logg.Log.Warn().Str("account", a.Name()).Time("until", until).Str("reason", reason).Msg("account resting")
	if err := p.Svc.Cooldown(ctx, a.Name(), until); err != nil {
		logg.Log.Error().Err(err).Str("account", a.Name()).Msg("could not set cooldown")
	}
}

func (p *MongoPool) Retire(ctx context.Context, a Account, group int, reason string) {
	logg.Log.Warn().Str("account", a.Name()).Int("group", group).Str("reason", reason).Msg("account retired")
	if err := p.Svc.Retire(ctx, a.Name(), group); err != nil {
		logg.Log.Error().Err(err).Str("account", a.Name()).Msg("could not retire account")
	}
}
