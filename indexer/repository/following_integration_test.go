package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

// Needs a MongoDB: MONGODB_TEST_URI=mongodb://127.0.0.1:27017 go test ./indexer/repository/
// It uses its own throwaway database and drops it afterwards.
func TestFollowingEdgesAreUpsertedNotDuplicated(t *testing.T) {
	uri := os.Getenv("MONGODB_TEST_URI")
	if uri == "" {
		t.Skip("MONGODB_TEST_URI not set")
	}
	conn := mongodb.NewConnection(&config.Persistence{MongoDB: config.MongoDB{URL: uri, Database: "following_test_" + time.Now().Format("150405")}})
	ctx := context.Background()
	defer conn.Datastore.Drop(ctx)
	repo := NewFollowingRepository(&conn)

	t1 := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	t2 := time.Now().Truncate(time.Millisecond)
	mk := func(at time.Time, name string) []*model.Following {
		return []*model.Following{
			model.NewFollowingDetailed("u1", "f1", "one", name, 10, 20, at),
			model.NewFollowingDetailed("u1", "f2", "two", "Two", 30, 40, at),
		}
	}
	if err := repo.UpsertEdges(ctx, mk(t1, "One")); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertEdges(ctx, mk(t2, "One Renamed")); err != nil { // same run again, later
		t.Fatal(err)
	}
	if n, _ := repo.CountByUserID(ctx, "u1"); n != 2 {
		t.Fatalf("rerun produced %d rows, want 2", n)
	}
	got, _ := repo.FindById(ctx, "u1_f1")
	if got == nil || got.FollowingName != "One Renamed" || !got.CapturedAt.Equal(t2) || got.FollowingFollowersCount != 10 {
		t.Fatalf("row not refreshed: %+v", got)
	}
	var raw bson.M
	_ = conn.Datastore.Collection(Collection).FindOne(ctx, bson.M{"_id": "u1_f1"}).Decode(&raw)
	if raw["first_captured_at"] == nil {
		t.Fatal("first_captured_at missing")
	}

	// the older refresh job writes bare edges; it must not blank the details
	if err := repo.UpdateOrInsert(ctx, model.NewFollowing("u1", "f1")); err != nil {
		t.Fatal(err)
	}
	got, _ = repo.FindById(ctx, "u1_f1")
	if got.FollowingHandle != "one" || got.FollowingName != "One Renamed" {
		t.Fatalf("bare UpdateOrInsert blanked the details: %+v", got)
	}

	// prune removes edges not seen by the latest complete run
	if err := repo.UpsertEdges(ctx, []*model.Following{model.NewFollowingDetailed("u1", "f3", "three", "Three", 1, 1, t1)}); err != nil {
		t.Fatal(err)
	}
	if n, _ := repo.DeleteStaleForUser(ctx, "u1", t2.Add(-time.Minute)); n != 1 {
		t.Fatalf("pruned %d, want 1", n)
	}
	if n, _ := repo.CountByUserID(ctx, "u1"); n != 2 {
		t.Fatalf("after prune %d rows, want 2", n)
	}
}
