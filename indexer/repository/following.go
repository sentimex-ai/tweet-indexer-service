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
)

const Collection = "following.indexes"

type FollowingRepository interface {
	UpdateOrInsert(ctx context.Context, pl *model.Following) error
	FindById(ctx context.Context, id string) (*model.Following, error)
	FindAllByUserID(ctx context.Context, userID string) ([]string, error)
	FindAllByFollowingID(ctx context.Context, followingID string) ([]string, error)
	DeleteAllByUserID(ctx context.Context, userID string) error
	InsertMultiple(ctx context.Context, followings []*model.Following) error
	FindAllUniqueFollowingIDs(ctx context.Context) ([]string, error)
	FindAllUniqueUserIDs(ctx context.Context) (map[string]struct{}, error)
	DeleteAllBeforeTime(ctx context.Context, beforeTime time.Time) (int64, error)
	ExistsByUserId(ctx context.Context, userID string) (bool, error)
	// given a user id get all followings minus the exclusions
	FollowingListWithExclusions(ctx context.Context, userID string, exclusions []string) ([]string, error)
	// given a following id get all users who follow it minus the exclusions
	FollowedByWithExclusions(ctx context.Context, followingID string, exclusions []string) ([]string, error)

	// UpsertEdges writes one page of edges in a single unordered bulk write,
	// keyed on _id = userID_followingID. first_seen_at is only set on insert.
	UpsertEdges(ctx context.Context, edges []*model.Following) (inserted, updated int64, err error)
	// PruneNotSeen deletes edges of userID that the given run did not see
	// (accounts the profile unfollowed). Only call after a *complete* scrape.
	PruneNotSeen(ctx context.Context, userID, runID string) (int64, error)
	CountByUserID(ctx context.Context, userID string) (int64, error)
	EnsureIndexes(ctx context.Context) error
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

func (repo *MongoRepository) UpdateOrInsert(ctx context.Context, pl *model.Following) error {
	err := repo.absrepo.InsertOrUpdate(pl)
	if err != nil {
		return err
	}
	return nil
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
