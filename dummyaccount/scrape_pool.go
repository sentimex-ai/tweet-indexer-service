package dummyaccount

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"github.com/tebeka/selenium"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ScrapePool leases accounts from dummy.accounts for the following scraper.
//
// Differences from the legacy LogInToNextGroup/LogOut pair:
//   - skips accounts that are cooling down after a rate limit
//   - skips accounts without cookies
//   - an in_use lease older than leaseTimeout is reclaimable, so a crashed
//     process does not strand accounts and nothing needs ResetAll() at boot
//     (ResetAll would also steal leases from other running instances)
//   - dead accounts are moved to a non-working group with the reason recorded
//     instead of being returned to the pool
type ScrapePool struct {
	coll         *mongo.Collection
	group        int
	leaseTimeout time.Duration
}

var ErrNoAccounts = errors.New("no usable accounts in pool")

func NewScrapePool(conn *mongodb.MongoConnection, group int, leaseTimeout time.Duration) *ScrapePool {
	if leaseTimeout <= 0 {
		leaseTimeout = 15 * time.Minute
	}
	return &ScrapePool{coll: conn.Datastore.Collection(Collection), group: group, leaseTimeout: leaseTimeout}
}

func (p *ScrapePool) usableFilter(now time.Time) bson.M {
	return bson.M{
		"group":     p.group,
		"cookies.0": bson.M{"$exists": true},
		"$and": bson.A{
			bson.M{"$or": bson.A{
				bson.M{"in_use": false},
				bson.M{"last_log_in": bson.M{"$lt": now.Add(-p.leaseTimeout)}},
			}},
			bson.M{"$or": bson.A{
				bson.M{"rate_limited_until": bson.M{"$exists": false}},
				bson.M{"rate_limited_until": bson.M{"$lte": now}},
			}},
		},
	}
}

// Acquire atomically leases the least recently used usable account.
// Returns ErrNoAccounts when nothing is usable right now; use NextAvailable
// to learn when a cooling-down account frees up.
func (p *ScrapePool) Acquire(ctx context.Context) (*SelAccount, error) {
	now := time.Now()
	update := bson.M{"$set": bson.M{"in_use": true, "last_log_in": now, "updated_at": now}}
	opts := options.FindOneAndUpdate().SetSort(bson.M{"last_log_in": 1}).SetReturnDocument(options.After)
	var acct SelAccount
	err := p.coll.FindOneAndUpdate(ctx, p.usableFilter(now), update, opts).Decode(&acct)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNoAccounts
	}
	if err != nil {
		return nil, err
	}
	return &acct, nil
}

// Release returns a healthy account to the pool.
func (p *ScrapePool) Release(ctx context.Context, username string) error {
	_, err := p.coll.UpdateOne(ctx, bson.M{"username": username},
		bson.M{"$set": bson.M{"in_use": false, "updated_at": time.Now()}})
	return err
}

// CoolDown releases the account and parks it until `until`.
func (p *ScrapePool) CoolDown(ctx context.Context, username string, until time.Time, reason string) error {
	now := time.Now()
	_, err := p.coll.UpdateOne(ctx, bson.M{"username": username}, bson.M{"$set": bson.M{
		"in_use": false, "rate_limited_until": until, "last_error": reason, "last_error_at": now, "updated_at": now,
	}})
	return err
}

// Retire moves a dead account out of the working group so it is not leased
// again. Cookies are kept so an operator can inspect/refresh them.
func (p *ScrapePool) Retire(ctx context.Context, username string, group int, reason string) error {
	now := time.Now()
	_, err := p.coll.UpdateOne(ctx, bson.M{"username": username}, bson.M{"$set": bson.M{
		"in_use": false, "group": group, "last_error": reason, "last_error_at": now, "updated_at": now,
	}})
	return err
}

// NextAvailable returns the earliest time a cooling-down account in this
// group becomes usable. ok=false means there are no such accounts at all.
func (p *ScrapePool) NextAvailable(ctx context.Context) (time.Time, bool, error) {
	opts := options.FindOne().SetSort(bson.M{"rate_limited_until": 1}).
		SetProjection(bson.M{"rate_limited_until": 1})
	var doc struct {
		Until time.Time `bson:"rate_limited_until"`
	}
	err := p.coll.FindOne(ctx, bson.M{
		"group": p.group, "cookies.0": bson.M{"$exists": true},
		"rate_limited_until": bson.M{"$gt": time.Now()},
	}, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return doc.Until, true, nil
}

// AccountSummary is a log-safe view of one pool account (no cookies).
type AccountSummary struct {
	UserName         string    `bson:"username"`
	Group            int       `bson:"group"`
	InUse            bool      `bson:"in_use"`
	CookieCount      int       `bson:"cookie_count"`
	RateLimitedUntil time.Time `bson:"rate_limited_until"`
	LastError        string    `bson:"last_error"`
	LastErrorAt      time.Time `bson:"last_error_at"`
	LastLoggedIn     time.Time `bson:"last_log_in"`
}

// List returns every account without cookie values.
func (p *ScrapePool) List(ctx context.Context) ([]AccountSummary, error) {
	cur, err := p.coll.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$project", Value: bson.M{
			"username": 1, "group": 1, "in_use": 1, "rate_limited_until": 1,
			"last_error": 1, "last_error_at": 1, "last_log_in": 1,
			"cookie_count": bson.M{"$size": bson.M{"$ifNull": bson.A{"$cookies", bson.A{}}}},
		}}},
		{{Key: "$sort", Value: bson.M{"group": 1, "username": 1}}},
	})
	if err != nil {
		return nil, err
	}
	var out []AccountSummary
	err = cur.All(ctx, &out)
	return out, err
}

// UpsertCookieAccount stores a session exported from a browser (no
// password/Selenium needed). Only auth_token, ct0 and twid are kept.
func (p *ScrapePool) UpsertCookieAccount(ctx context.Context, username, authToken, ct0, twid string) error {
	username = strings.TrimPrefix(strings.TrimSpace(username), "@")
	if username == "" || authToken == "" || ct0 == "" {
		return errors.New("username, auth_token and ct0 are required")
	}
	exp := uint(time.Now().AddDate(1, 0, 0).Unix())
	cookies := []selenium.Cookie{
		{Name: "auth_token", Value: authToken, Domain: ".x.com", Path: "/", Secure: true, Expiry: exp},
		{Name: "ct0", Value: ct0, Domain: ".x.com", Path: "/", Secure: true, Expiry: exp},
	}
	if twid != "" {
		cookies = append(cookies, selenium.Cookie{Name: "twid", Value: twid, Domain: ".x.com", Path: "/", Secure: true, Expiry: exp})
	}
	now := time.Now()
	// Legacy code keys this collection on _id == username (SelAccount.GetID).
	_, err := p.coll.UpdateOne(ctx, bson.M{"_id": username}, bson.M{
		"$set": bson.M{
			"username": username, "cookies": cookies, "group": p.group, "in_use": false, "updated_at": now,
			"last_error": "", "rate_limited_until": time.Time{},
		},
		"$setOnInsert": bson.M{
			"password": "", "bought_from": "manual-cookies", "in_grok_use": false,
			"last_log_in": time.Time{}, "last_grok": time.Time{},
		},
	}, options.Update().SetUpsert(true))
	return err
}
