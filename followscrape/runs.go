package followscrape

import (
	"context"
	"errors"

	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const RunsCollection = "following.runs"

// MongoRuns persists Run documents (one per scrape attempt of a handle).
type MongoRuns struct{ coll *mongo.Collection }

func NewMongoRuns(conn *mongodb.MongoConnection) *MongoRuns {
	return &MongoRuns{coll: conn.Datastore.Collection(RunsCollection)}
}

func (m *MongoRuns) EnsureIndexes(ctx context.Context) error {
	_, err := m.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "handle", Value: 1}, {Key: "started_at", Value: -1}},
	})
	return err
}

func (m *MongoRuns) Save(ctx context.Context, r *Run) error {
	_, err := m.coll.ReplaceOne(ctx, bson.M{"_id": r.ID}, r, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoRuns) Get(ctx context.Context, id string) (*Run, error) {
	var r Run
	err := m.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return &r, err
}

// LatestResumable returns the newest partial/failed run of handle that has a
// cursor checkpoint, or nil.
func (m *MongoRuns) LatestResumable(ctx context.Context, handle string) (*Run, error) {
	var r Run
	err := m.coll.FindOne(ctx,
		bson.M{"handle": handle, "status": bson.M{"$in": bson.A{StatusPartial, StatusFailed, StatusRunning}}, "last_cursor": bson.M{"$ne": ""}},
		options.FindOne().SetSort(bson.M{"started_at": -1}),
	).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (m *MongoRuns) Recent(ctx context.Context, limit int64) ([]*Run, error) {
	cur, err := m.coll.Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"started_at": -1}).SetLimit(limit))
	if err != nil {
		return nil, err
	}
	var out []*Run
	err = cur.All(ctx, &out)
	return out, err
}
