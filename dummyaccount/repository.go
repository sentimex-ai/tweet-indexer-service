package dummyaccount

import (
	"context"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/logg"
	"github.com/jeetdotso/tweet-indexer-service/persistence"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Collection = "dummy.accounts"

type Repository interface {
	UpdateOrInsert(ctx context.Context, pl *SelAccount) error
	FindById(ctx context.Context, id string) (*SelAccount, error)
	FindNext(ctx context.Context) (*SelAccount, error)
	FindNextByGroup(ctx context.Context, group int) (*SelAccount, error)
	FindInUse(ctx context.Context, group int) (*SelAccount, error)
	FindGroupNumber(ctx context.Context) (int, error)
	ResetAll(ctx context.Context) error
	FindAllByGroup(ctx context.Context, group int) ([]*SelAccount, error)
	UpdateAllToGroup(ctx context.Context, group int) error
	AcquireNextByGroup(ctx context.Context, group int) (*SelAccount, error)
	AcquireNextGrokByGroup(ctx context.Context, group int) (*SelAccount, error)
	ReleaseGrok(ctx context.Context, username string) error
	// SetCooldown frees an account (in_use=false) and keeps it out of AcquireNextByGroup until `until`.
	SetCooldown(ctx context.Context, username string, until time.Time) error
}
type MongoRepository struct {
	conn    *mongodb.MongoConnection
	absrepo *persistence.AbstractMongoRepository[*SelAccount]
}

func NewRepository(conn *mongodb.MongoConnection) Repository {
	absrepo := persistence.NewAbstractRepository[*SelAccount](conn, Collection)
	return &MongoRepository{
		conn:    conn,
		absrepo: absrepo,
	}
}

func (repo *MongoRepository) UpdateOrInsert(ctx context.Context, pl *SelAccount) error {
	err := repo.absrepo.InsertOrUpdate(pl)
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepository) FindById(ctx context.Context, id string) (*SelAccount, error) {
	filter := bson.M{"username": id}
	var pool SelAccount
	res := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter)
	if err := res.Decode(&pool); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Error decoding scraper account.")
		return nil, err
	}
	return &pool, nil
}

func (repo *MongoRepository) FindNext(ctx context.Context) (*SelAccount, error) {
	// find next (last updated account) in group 0
	filter := bson.M{
		"in_use": false,
		"group":  bson.M{"$in": bson.A{0}},
	}

	opts := options.FindOne().SetSort(bson.M{"updated_at": 1})

	var account SelAccount
	err := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter, opts).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	return &account, nil
}
func (repo *MongoRepository) FindNextByGroup(ctx context.Context, group int) (*SelAccount, error) {
	// find next (last updated account) in group 0
	filter := bson.M{
		"in_use": false,
		"group":  bson.M{"$in": bson.A{group}},
	}

	opts := options.FindOne().SetSort(bson.M{"updated_at": 1})

	var account SelAccount
	err := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter, opts).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	return &account, nil
}

func (repo *MongoRepository) AcquireNextByGroup(ctx context.Context, group int) (*SelAccount, error) {
	// 1) Filter for group membership and in_use=false
	filter := bson.M{
		"in_use": false,
		"group":  bson.M{"$in": bson.A{group}},
		// skip accounts resting after a rate limit (rows that never had a cooldown match too)
		"$or": bson.A{
			bson.M{"cooldown_until": bson.M{"$exists": false}},
			bson.M{"cooldown_until": bson.M{"$lte": time.Now()}},
		},
	}

	// 2) Update sets in_use=true and bumps timestamps
	update := bson.M{"$set": bson.M{
		"in_use":      true,
		"last_log_in": time.Now(),
		"updated_at":  time.Now(),
	}}

	// 3) We want to pick the lowest-last_log_in first:
	opts := options.FindOneAndUpdate().
		SetSort(bson.M{"last_log_in": 1}).
		SetReturnDocument(options.After) // return the doc *after* update

	var acct SelAccount
	err := repo.conn.Datastore.
		Collection(repo.absrepo.Collection).
		FindOneAndUpdate(ctx, filter, update, opts).
		Decode(&acct)

	if err == mongo.ErrNoDocuments {
		return nil, nil // no free accounts
	}
	if err != nil {
		return nil, err
	}
	return &acct, nil
}
func (repo *MongoRepository) AcquireNextGrokByGroup(ctx context.Context, group int) (*SelAccount, error) {
	// 1) Filter for group membership and in_use=false
	filter := bson.M{
		"group":       bson.M{"$in": bson.A{group}},
		"in_grok_use": false,
	}

	// 2) Update sets last_grok and bumps timestamps
	update := bson.M{"$set": bson.M{
		"last_grok":   time.Now(),
		"in_grok_use": true,
		"updated_at":  time.Now(),
	}}

	// 3) We want to pick the lowest-last_grok first:
	opts := options.FindOneAndUpdate().
		SetSort(bson.M{"last_grok": 1}).
		SetReturnDocument(options.After) // return the doc *after* update

	var acct SelAccount
	err := repo.conn.Datastore.
		Collection(repo.absrepo.Collection).
		FindOneAndUpdate(ctx, filter, update, opts).
		Decode(&acct)

	if err == mongo.ErrNoDocuments {
		return nil, nil // no free accounts
	}
	if err != nil {
		return nil, err
	}
	return &acct, nil
}
func (repo *MongoRepository) FindInUse(ctx context.Context, group int) (*SelAccount, error) {
	filter := bson.M{"in_use": true, "group": group}

	var account SelAccount
	err := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	return &account, nil
}

func (repo *MongoRepository) FindGroupNumber(ctx context.Context) (int, error) {
	filter := bson.M{}

	opts := options.FindOne().SetSort(bson.M{"group": -1})

	var selAccount SelAccount

	err := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter, opts).Decode(&selAccount)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}

	return selAccount.Group, nil
}

func (repo *MongoRepository) ResetAll(ctx context.Context) error {
	filter := bson.M{}

	update := bson.M{
		"$set": bson.M{
			"in_use":      false,
			"in_grok_use": false,
			"updated_at":  time.Now(),
		},
	}

	opts := options.Update()

	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).UpdateMany(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	return nil
}

func (repo *MongoRepository) FindAllByGroup(ctx context.Context, group int) ([]*SelAccount, error) {
	filter := bson.M{"group": group}

	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var accounts []*SelAccount
	for cursor.Next(ctx) {
		var account SelAccount
		if err := cursor.Decode(&account); err != nil {
			return nil, err
		}
		accounts = append(accounts, &account)
	}

	return accounts, nil
}

func (repo *MongoRepository) UpdateAllToGroup(ctx context.Context, group int) error {
	filter := bson.M{}

	update := bson.M{
		"$set": bson.M{
			"group":      group,
			"updated_at": time.Now(),
		},
	}

	opts := options.Update()

	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).UpdateMany(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoRepository) ReleaseGrok(ctx context.Context, username string) error {
	res, err := r.conn.Datastore.Collection(r.absrepo.Collection).UpdateOne(
		ctx,
		bson.M{"username": username, "in_grok_use": true}, // only if actually in use
		bson.M{
			"$set":         bson.M{"in_grok_use": false},
			"$currentDate": bson.M{"updated_at": true},
		},
		// Consider enabling retryable writes at client level
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		// Optional: not an error, but useful to know (double release / wrong username)
		logg.Log.Warn().Str("username", username).Msg("ReleaseGrok: no matching in-use account")
	}
	return nil
}

func (repo *MongoRepository) SetCooldown(ctx context.Context, username string, until time.Time) error {
	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).UpdateOne(ctx,
		bson.M{"username": username},
		bson.M{"$set": bson.M{"in_use": false, "cooldown_until": until, "updated_at": time.Now()}})
	return err
}
