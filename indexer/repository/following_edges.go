package repository

import (
	"context"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (repo *MongoRepository) coll() *mongo.Collection {
	return repo.conn.Datastore.Collection(repo.absrepo.Collection)
}

func (repo *MongoRepository) UpsertEdges(ctx context.Context, edges []*model.Following) (int64, int64, error) {
	if len(edges) == 0 {
		return 0, 0, nil
	}
	writes := make([]mongo.WriteModel, 0, len(edges))
	for _, e := range edges {
		if e.ID == "" {
			e.ID = model.FollowingID(e.UserID, e.FollowingID)
		}
		first := e.FirstSeenAt
		if first.IsZero() {
			first = e.CapturedAt
		}
		writes = append(writes, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": e.ID}).
			SetUpsert(true).
			SetUpdate(bson.M{
				"$set": bson.M{
					"user_id":         e.UserID,
					"following_id":    e.FollowingID,
					"source_handle":   e.SourceHandle,
					"handle":          e.Handle,
					"display_name":    e.DisplayName,
					"follower_count":  e.FollowerCount,
					"following_count": e.FollowingCount,
					"verified":        e.Verified,
					"protected":       e.Protected,
					"profile_img":     e.ProfileImg,
					"captured_at":     e.CapturedAt,
					"run_id":          e.RunID,
					"updated_at":      e.UpdatedAt,
				},
				"$setOnInsert": bson.M{"first_seen_at": first},
			}))
	}
	res, err := repo.coll().BulkWrite(ctx, writes, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}
	return res.UpsertedCount, res.MatchedCount, nil
}

func (repo *MongoRepository) PruneNotSeen(ctx context.Context, userID, runID string) (int64, error) {
	if userID == "" || runID == "" {
		return 0, nil
	}
	res, err := repo.coll().DeleteMany(ctx, bson.M{"user_id": userID, "run_id": bson.M{"$ne": runID}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

func (repo *MongoRepository) CountByUserID(ctx context.Context, userID string) (int64, error) {
	return repo.coll().CountDocuments(ctx, bson.M{"user_id": userID})
}

func (repo *MongoRepository) EnsureIndexes(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := repo.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "run_id", Value: 1}}},
		{Keys: bson.D{{Key: "following_id", Value: 1}}},
		{Keys: bson.D{{Key: "source_handle", Value: 1}}},
	})
	return err
}
