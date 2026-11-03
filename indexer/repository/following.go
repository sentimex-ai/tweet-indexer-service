package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/persistence"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Collection = "following.indexes"

type FollowingRepository interface {
	UpdateOrInsert(ctx context.Context, pl *model.Following) error
	FindById(ctx context.Context, id string) (*model.Following, error)
	FindAllByUserID(ctx context.Context, userID string) ([]string, error)
	FindAllByFollowingID(ctx context.Context, followingID string) ([]string, error)
	DeleteAllByUserID(ctx context.Context, userID string) error
	// UpsertEdges writes edges by _id (userID_followingID): a repeat run updates
	// the row and never adds a second one. Safe to call with up to a page of edges.
	UpsertEdges(ctx context.Context, edges []*model.Following) error
	// CountByUserID is the number of stored edges for one source user.
	CountByUserID(ctx context.Context, userID string) (int64, error)
	// DeleteStaleForUser removes a user's edges last captured before cutoff
	// (accounts they no longer follow). Only call after a complete run.
	DeleteStaleForUser(ctx context.Context, userID string, cutoff time.Time) (int64, error)
	InsertMultiple(ctx context.Context, followings []*model.Following) error
	FindAllUniqueFollowingIDs(ctx context.Context) ([]string, error)
	FindAllUniqueUserIDs(ctx context.Context) (map[string]struct{}, error)
	DeleteAllBeforeTime(ctx context.Context, beforeTime time.Time) (int64, error)
	ExistsByUserId(ctx context.Context, userID string) (bool, error)
	// given a user id get all followings minus the exclusions
	FollowingListWithExclusions(ctx context.Context, userID string, exclusions []string) ([]string, error)
	// given a following id get all users who follow it minus the exclusions
	FollowedByWithExclusions(ctx context.Context, followingID string, exclusions []string) ([]string, error)
}
type MongoRepository struct {
	conn    *mongodb.MongoConnection
	absrepo *persistence.AbstractMongoRepository[*model.Following]
}

func NewFollowingRepository(conn *mongodb.MongoConnection) FollowingRepository {
	absrepo := persistence.NewAbstractRepository[*model.Following](conn, Collection)
	return &MongoRepository{
		conn:    conn,
		absrepo: absrepo,
	}
}

// UpdateOrInsert upserts the bare edge. It sets only the edge fields, so it
// cannot blank the details the on-demand scraper stored on the same row.
func (repo *MongoRepository) UpdateOrInsert(ctx context.Context, pl *model.Following) error {
	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).UpdateOne(ctx,
		bson.M{"_id": pl.ID},
		bson.M{"$set": bson.M{"user_id": pl.UserID, "following_id": pl.FollowingID, "updated_at": pl.UpdatedAt}},
		options.Update().SetUpsert(true))
	return err
}

func (repo *MongoRepository) UpsertEdges(ctx context.Context, edges []*model.Following) error {
	if len(edges) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(edges))
	for _, e := range edges {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": e.ID}).
			SetUpdate(bson.M{
				"$set": bson.M{
					"user_id":                   e.UserID,
					"following_id":              e.FollowingID,
					"following_handle":          e.FollowingHandle,
					"following_name":            e.FollowingName,
					"following_followers_count": e.FollowingFollowersCount,
					"following_following_count": e.FollowingFollowingCount,
					"captured_at":               e.CapturedAt,
					"updated_at":                e.UpdatedAt,
				},
				"$setOnInsert": bson.M{"first_captured_at": e.CapturedAt},
			}).
			SetUpsert(true))
	}
	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).
		BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

func (repo *MongoRepository) CountByUserID(ctx context.Context, userID string) (int64, error) {
	return repo.conn.Datastore.Collection(repo.absrepo.Collection).CountDocuments(ctx, bson.M{"user_id": userID})
}

func (repo *MongoRepository) DeleteStaleForUser(ctx context.Context, userID string, cutoff time.Time) (int64, error) {
	res, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).
		DeleteMany(ctx, bson.M{"user_id": userID, "captured_at": bson.M{"$lt": cutoff}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

func (repo *MongoRepository) FindById(ctx context.Context, id string) (*model.Following, error) {
	filter := bson.M{"_id": id}
	var following model.Following
	res := repo.conn.Datastore.Collection(repo.absrepo.Collection).FindOne(ctx, filter)
	if err := res.Decode(&following); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &following, nil
}

func (repo *MongoRepository) FindAllByUserID(ctx context.Context, userID string) ([]string, error) {
	filter := bson.M{"user_id": userID}
	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var followings []string
	for cursor.Next(ctx) {
		var following model.Following
		if err := cursor.Decode(&following); err != nil {
			return nil, err
		}
		followings = append(followings, following.FollowingID)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return followings, nil
}

func (repo *MongoRepository) FindAllByFollowingID(ctx context.Context, followingID string) ([]string, error) {
	filter := bson.M{"following_id": followingID}
	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var userIDs []string
	for cursor.Next(ctx) {
		var following model.Following
		if err := cursor.Decode(&following); err != nil {
			return nil, err
		}
		userIDs = append(userIDs, following.UserID)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return userIDs, nil
}

func (repo *MongoRepository) DeleteAllByUserID(ctx context.Context, userID string) error {
	filter := bson.M{"user_id": userID}
	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepository) InsertMultiple(ctx context.Context, followings []*model.Following) error {
	if len(followings) == 0 {
		return nil // No followings to insert
	}

	docs := make([]interface{}, len(followings))
	for i, following := range followings {
		docs[i] = following
	}

	_, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).InsertMany(ctx, docs)
	if err != nil {
		return err
	}
	return nil
}

func (repo *MongoRepository) FindAllUniqueFollowingIDs(ctx context.Context) ([]string, error) {
	filter := bson.M{}
	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Distinct(ctx, "following_id", filter)
	if err != nil {
		return nil, err
	}

	var followingIDs []string
	for _, id := range cursor {
		if idStr, ok := id.(string); ok {
			followingIDs = append(followingIDs, idStr)
		}
	}

	return followingIDs, nil
}

func (repo *MongoRepository) FindAllUniqueUserIDs(ctx context.Context) (map[string]struct{}, error) {
	filter := bson.M{}
	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Distinct(ctx, "user_id", filter)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]struct{})
	for _, id := range cursor {
		if idStr, ok := id.(string); ok {
			userIDs[idStr] = struct{}{}
		}
	}

	return userIDs, nil
}

func (repo *MongoRepository) DeleteAllBeforeTime(ctx context.Context, beforeTime time.Time) (int64, error) {
	filter := bson.M{
		"updated_at": bson.M{"$lt": beforeTime},
	}

	result, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete failed: %w", err)
	}

	return result.DeletedCount, nil
}

func (repo *MongoRepository) ExistsByUserId(ctx context.Context, userID string) (bool, error) {
	filter := bson.M{"user_id": userID}
	count, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).CountDocuments(ctx, filter)
	if err != nil {
		return false, fmt.Errorf("count failed: %w", err)
	}

	return count > 0, nil
}

func (repo *MongoRepository) FollowingListWithExclusions(ctx context.Context, userID string, exclusions []string) ([]string, error) {
	filter := bson.M{
		"user_id": userID,
	}

	if len(exclusions) > 0 {
		filter["following_id"] = bson.M{"$nin": exclusions}
	}

	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var followings []string
	for cursor.Next(ctx) {
		var following model.Following
		if err := cursor.Decode(&following); err != nil {
			return nil, err
		}
		followings = append(followings, following.FollowingID)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return followings, nil
}

func (repo *MongoRepository) FollowedByWithExclusions(ctx context.Context, followingID string, exclusions []string) ([]string, error) {
	filter := bson.M{
		"following_id": followingID,
	}

	if len(exclusions) > 0 {
		filter["user_id"] = bson.M{"$nin": exclusions}
	}

	cursor, err := repo.conn.Datastore.Collection(repo.absrepo.Collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var userIDs []string
	for cursor.Next(ctx) {
		var following model.Following
		if err := cursor.Decode(&following); err != nil {
			return nil, err
		}
		userIDs = append(userIDs, following.UserID)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return userIDs, nil
}
