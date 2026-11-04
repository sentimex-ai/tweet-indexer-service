package service

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/indexer/repository"
	"github.com/jeetdotso/tweet-indexer-service/logg"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	twitterTypes "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/types"
	"github.com/jeetdotso/tweet-indexer-service/scrape/tracking"
	"github.com/jeetdotso/tweet-indexer-service/stream"
	"github.com/jeetdotso/tweet-indexer-service/stream/kafka"
	"github.com/jeetdotso/tweet-indexer-service/twitterprofile"
	"github.com/jeetdotso/tweet-indexer-service/util"
	"google.golang.org/protobuf/proto"
)

// receive who's following by followed id
// following list by id
// following list by id true | false if tracked
// get followed by list via id w/ excluding ids
type FollowingIndexer struct {
	twtPrf         twitterprofile.Repository
	prfSvc         Profile
	dummSvc        dummyaccount.Service
	handleCounts   sync.Map // map[string]*atomic.Int64
	follRepo       repository.FollowingRepository
	URL            string // URL for the grok service, can be set in config
	followingTopic string
	followProducer stream.Producer
}

const (
	FOLLOW_KEY                = "followings"
	NEW_USER_KEY              = "new_user"
	SINGLE_PAGE_WORKER_COUNT  = 10
	SINGLE_PAGE_HANDLER_COUNT = 20
	SINGLE_PAGE_MAX_RETRIES   = 3
	SINGLE_PAGE_RETRY_DELAY   = 3 * time.Second

	FOLLOWING_WORKER_COUNT  = 30
	FOLLOWING_HANDLER_COUNT = 30
	FOLLOWING_MAX_RETRIES   = 3
	FOLLOWING_RETRY_DELAY   = 3 * time.Second

	SINGLE_USER_WORKER_COUNT  = 1
	SINGLE_USER_HANDLER_COUNT = 5
	SINGLE_USER_MAX_RETRIES   = 4
	SINGLE_USER_RETRY_DELAY   = 2 * time.Second

	THRESHOLD_FOLLOWING_COUNT = 15000 // Threshold for the following count for saving
	FOLLOW_TOPIC_NAME         = "followers"
)

func NewFollowingIndexer(cfg config.Config, conn *mongodb.MongoConnection, dummySvc dummyaccount.Service) FollowingIndexer {
	follRepo := repository.NewFollowingRepository(conn)
	twtPrf := twitterprofile.NewRepository(conn)
	prfSvc := NewProfile(cfg, conn, dummySvc)
	follPro := kafka.NewProducerIfEnabled(cfg.Stream, cfg.Stream.Kafka.FollowingTopic)

	return FollowingIndexer{
		follRepo:       follRepo,
		dummSvc:        dummySvc,
		twtPrf:         twtPrf,
		prfSvc:         prfSvc,
		handleCounts:   sync.Map{}, // map[string]*atomic.Int64
		URL:            cfg.Services.GrokAPIServices.Host,
		followProducer: follPro,
		followingTopic: cfg.Stream.Kafka.FollowingTopic,
	}
}

func (f *FollowingIndexer) TopicNames() []string {
	return []string{f.followingTopic}
}

func (f *FollowingIndexer) ScrapeFollowingList(ctx context.Context) error {
	startTime := time.Now()
	userIds, err := f.follRepo.FindAllUniqueUserIDs(ctx)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to find all unique user IDs.")
		return err
	}
	if len(userIds) == 0 {
		logg.Log.Info().Caller().Ctx(ctx).Msg("No unique user IDs found, skipping.")
		return nil
	}

	tracking.MonitorFollowingsPipeline(ctx, userIds, min(FOLLOWING_WORKER_COUNT, len(userIds)), min(FOLLOWING_HANDLER_COUNT, len(userIds)), FOLLOWING_MAX_RETRIES, FOLLOWING_RETRY_DELAY, f.HandleFollowing, f.dummSvc)
	_, err = f.follRepo.DeleteAllBeforeTime(ctx, startTime) // Clean up old followings
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to delete old followings.")
		return err
	}
	ctl := util.CreateBackgroundContextFromExisting(ctx)
	go f.streamAllFollowings(ctl) // Stream all followings after processing
	return nil
}

func (f *FollowingIndexer) ScrapeFollowingListById(ctx context.Context, userId string) {
	if userId == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("User ID is empty, skipping.")
		return
	}
	//check if userId already exists in the following repo
	exists, err := f.follRepo.ExistsByUserId(ctx, userId)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to check if user ID exists in following repo.")
		return
	}
	if exists {
		logg.Log.Info().Caller().Ctx(ctx).Msgf("User ID %s already exists in following repo, skipping.", userId)
		if err := f.streamFollowingsByUserID(ctx, userId, NEW_USER_KEY); err != nil {
			logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to stream followings by user ID.")
		}
		return
	}
	userIds := map[string]struct{}{userId: {}}
	tracking.MonitorFollowingsPipeline(ctx, userIds, SINGLE_USER_WORKER_COUNT, SINGLE_PAGE_HANDLER_COUNT, SINGLE_USER_MAX_RETRIES, SINGLE_USER_RETRY_DELAY, f.HandleFollowing, f.dummSvc)
	if err := f.streamFollowingsByUserID(ctx, userId, NEW_USER_KEY); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to stream followings by user ID.")
	}
}

func (f *FollowingIndexer) ScrapeFollowingListBySinglePage(ctx context.Context) error {
	userIds, err := f.follRepo.FindAllUniqueUserIDs(ctx)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to find all unique user IDs.")
		return err
	}
	if len(userIds) == 0 {
		logg.Log.Info().Caller().Ctx(ctx).Msg("No unique user IDs found, skipping.")
		return nil
	}
	tracking.MonitorSingleFollowingPagePipeline(ctx, userIds, min(SINGLE_PAGE_WORKER_COUNT, len(userIds)), min(SINGLE_PAGE_HANDLER_COUNT, len(userIds)), SINGLE_PAGE_MAX_RETRIES, SINGLE_USER_RETRY_DELAY, f.HandleFollowing, f.dummSvc)
	ctl := util.CreateBackgroundContextFromExisting(ctx)
	go f.streamAllFollowings(ctl) // Stream all followings after processing
	return nil
}

func (f *FollowingIndexer) HandleFollowing(ctx context.Context, followedByID string, user twitterTypes.FollowingUser) {
	if user.ID == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("User ID is empty, skipping.")
		return
	}
	//add to following repo
	// Write the full snapshot through the same upsert the following scraper
	// uses; the reflective UpdateOrInsert would $set the snapshot fields to
	// zero values.
	follow := model.NewFollowing(followedByID, user.ID)
	follow.Handle, follow.DisplayName = user.Handle, user.UserName
	follow.FollowerCount, follow.FollowingCount = user.Followers, user.Following
	follow.Verified, follow.ProfileImg = user.Verified, user.ProfileImg
	follow.CapturedAt = follow.UpdatedAt
	if _, _, err := f.follRepo.UpsertEdges(ctx, []*model.Following{follow}); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to upsert following.")
		return
	}
	//add to twt prf
	if err := f.twtPrf.UpsertFetchedProfile(ctx, user); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to upsert twitter profile.")
		return
	}
}

func (f *FollowingIndexer) CountsSnapshot() map[string]int64 {
	out := make(map[string]int64)
	f.handleCounts.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}
func (f *FollowingIndexer) ScrapeFollowingListByIdForAdmin(ctx context.Context, userId string) {
	if userId == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("User ID is empty, skipping.")
		return
	}
	//check if userId already exists in the following repo
	exists, err := f.follRepo.ExistsByUserId(ctx, userId)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to check if user ID exists in following repo.")
		return
	}
	if exists {
		logg.Log.Info().Caller().Ctx(ctx).Msgf("User ID %s already exists in following repo, skipping.", userId)
		return
	}
	userIds := map[string]struct{}{userId: {}}
	tracking.MonitorFollowingsPipeline(ctx, userIds, SINGLE_USER_WORKER_COUNT, SINGLE_PAGE_HANDLER_COUNT, SINGLE_USER_MAX_RETRIES, SINGLE_USER_RETRY_DELAY, f.HandleManualFollowings, f.dummSvc)
}

func (f *FollowingIndexer) HandleManualFollowings(ctx context.Context, followedByID string, user twitterTypes.FollowingUser) {
	if user.ID == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("User ID is empty, skipping.")
		return
	}
	if user.Followers < THRESHOLD_FOLLOWING_COUNT {
		return // Skip if followers count is below threshold
	}

	//add to fetched prf
	if err := f.twtPrf.UpsertFetchedProfile(ctx, user); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to upsert twitter profile.")
		return
	}
	// ctl := util.CreateBackgroundContextFromExisting(ctx)
	// go f.RunGrokOnPrf(ctl, user.ID, user.Handle)
	return
}

func (f *FollowingIndexer) GetByUsernameAndSaveFollowings(ctx context.Context, username string) error {
	if username == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("Username is empty, skipping.")
		return nil
	}
	user, err := f.prfSvc.GetProfileByUsername(ctx, username)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to get user ID by username.")
		return err
	}
	if user == nil || user.ID == "" {
		logg.Log.Error().Caller().Ctx(ctx).Err(ErrProfileNotFound).Msgf("User ID for %s is empty, skipping.", username)
		return ErrProfileNotFound
	}
	ctu := util.CreateBackgroundContextFromExisting(ctx)
	go f.ScrapeFollowingListByIdForAdmin(ctu, user.ID)
	return nil
}
func (f *FollowingIndexer) GetByUsernamesAndCheckMulti(ctx context.Context, usernames []string) error {
	if len(usernames) == 0 {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("Username is empty, skipping.")
		return nil
	}
	sem := make(chan struct{}, 10)
	ctl := util.CreateBackgroundContextFromExisting(ctx)
	for _, username := range usernames {
		if username == "" {
			logg.Log.Warn().Caller().Ctx(ctx).Msg("Username is empty, skipping.")
			continue
		}
		sem <- struct{}{}
		go func(ctx context.Context, un string) {
			defer func() { <-sem }()
			//check repo
			exists, err := f.twtPrf.FindFetchedUserByTwitterHandle(ctx, un)
			if err != nil {
				logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("Failed to check if user %s exists in twitter profile repo.", un)
				return
			}
			if exists != nil {
				logg.Log.Info().Caller().Ctx(ctx).Msgf("User %s already exists in twitter profile repo, skipping.", un)
				return
			}
			user, err := f.prfSvc.GetProfileByUsername(ctx, un)
			if err != nil {
				logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to get user ID by username.")
				return
			}
			if user == nil || user.ID == "" {
				logg.Log.Error().Caller().Ctx(ctx).Err(ErrProfileNotFound).Msgf("User ID for %s is empty, skipping.", un)
				return
			}
			fl := user.ToFollowingUser()
			f.HandleManualFollowings(ctx, user.ID, fl)
		}(ctl, username)
	}
	// Wait for all goroutines to finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
	return nil
}

func (f *FollowingIndexer) RunGrokOnPrf(ctx context.Context, userId, userName string) {

	tracking.GetGrokProfile(ctx, userId, userName, f.dummSvc, f.URL, f.HandleGrokProfile)
}

func (f *FollowingIndexer) HandleGrokProfile(ctx context.Context, id string, tag string) {
	if id == "" || tag == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("ID or tag is empty, skipping.")
		return
	}
	rTag := strings.ToLower(strings.TrimSpace(tag))
	if !contains(rTag) {
		rTag = twitterprofile.TAG_OTHER.String() // Default to other if tag is not recognized
	}

	if err := f.twtPrf.UpdateTagById(ctx, id, rTag); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("Failed to update tag for ID: %s", id)
		return
	}
}

func (f *FollowingIndexer) GetEmptyTagsAndRunGrok(ctx context.Context) error {
	// 1) fetch inputs (honor ctx)
	profiles, err := f.twtPrf.GetAllByTag(ctx, "")
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to find profiles with empty tags.")
		return err
	}
	if len(profiles) == 0 {
		logg.Log.Info().Caller().Ctx(ctx).Msg("No profiles with empty tags found, skipping.")
		return nil
	}

	// 2) worker pool
	jobs := make(chan *twitterprofile.FetchedProfile, SINGLE_PAGE_WORKER_COUNT)
	var wg sync.WaitGroup

	for i := 0; i < SINGLE_PAGE_WORKER_COUNT/2; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for p := range jobs {
				// per-profile timeout (independent so we can drain gracefully)
				taskCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

				f.RunGrokOnPrf(taskCtx, p.ID, p.UserName)

				// decide how it ended
				select {
				case <-taskCtx.Done():
					switch taskCtx.Err() {
					case context.DeadlineExceeded:
						logg.Log.Warn().
							Int("worker", workerID).
							Str("user", p.UserName).
							Str("userID", p.ID).
							Msg("Grok profile timed out")
					case context.Canceled:
						logg.Log.Warn().
							Int("worker", workerID).
							Str("user", p.UserName).
							Str("userID", p.ID).
							Msg("Grok profile canceled")
					default:
						logg.Log.Warn().
							Int("worker", workerID).
							Str("user", p.UserName).
							Str("userID", p.ID).
							Err(taskCtx.Err()).
							Msg("Grok profile stopped due to context error")
					}
				}
				cancel()
			}
		}(i)
	}

	// 3) enqueue (stop if parent ctx is canceled)
	for _, p := range profiles {
		select {
		case <-ctx.Done():
			logg.Log.Warn().Msg("shutdown/canceled - stop enqueueing profiles")
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- p:
		}
	}
	close(jobs)
	wg.Wait()
	return nil
}

func (f *FollowingIndexer) streamAllFollowings(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logg.Log.Error().
				Caller().
				Ctx(ctx).
				Msgf("Recovered from panic in streamAllFollowings: %v\n%s", r, debug.Stack())
		}
	}()

	userIds, err := f.follRepo.FindAllUniqueUserIDs(ctx)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to find all unique user IDs.")
		return
	}
	if len(userIds) == 0 {
		logg.Log.Info().Caller().Ctx(ctx).Msg("No unique user IDs found, skipping.")
		return
	}

	for userID := range userIds {
		if err := f.streamFollowingsByUserID(ctx, userID, FOLLOW_KEY); err != nil {
			logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("Failed to stream followings for user ID: %s", userID)
			continue
		}
	}
	return
}

func (f *FollowingIndexer) streamFollowingsByUserID(ctx context.Context, userID, key string) error {
	defer func() {
		if r := recover(); r != nil {
			logg.Log.Error().
				Caller().
				Ctx(ctx).
				Msgf("Recovered from panic in streamFollowingsByUserID for user %s: %v\n%s",
					userID, r, debug.Stack(),
				)
		}
	}()

	if userID == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("UserID is empty, skipping.")
		return nil
	}
	followings, err := f.follRepo.FindAllByUserID(ctx, userID)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("Failed to find followings for user ID: %s", userID)
		return err
	}
	if len(followings) == 0 {
		logg.Log.Info().Caller().Ctx(ctx).Msgf("No followings found for user ID: %s", userID)
		return nil
	}

	b, err := proto.Marshal(model.ToProto(userID, followings))
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to marshal proto message.")
		return err
	}
	if err = f.followProducer.Produce([]byte(key), b); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to produce followings stream message.")
		return err

	}
	return nil

}

func (f *FollowingIndexer) GetByUsernameAndAdd(ctx context.Context, username, tag string) error {
	if username == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("Username is empty, skipping.")
		return nil
	}

	if username == "" {
		logg.Log.Warn().Caller().Ctx(ctx).Msg("Username is empty, skipping.")
		return fmt.Errorf("username is empty")
	}
	//check repo
	exists, err := f.twtPrf.FindFetchedUserByTwitterHandle(ctx, username)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("Failed to check if user %s exists in twitter profile repo.", username)
		return err
	}
	if exists != nil {
		logg.Log.Info().Caller().Ctx(ctx).Msgf("User %s already exists in twitter profile repo, skipping.", username)
		return ErrProfileAlreadyExists
	}
	user, err := f.prfSvc.GetProfileByUsername(ctx, username)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to get user ID by username.")
		return ErrProfileNotFound
	}
	if user == nil || user.ID == "" {
		logg.Log.Error().Caller().Ctx(ctx).Err(ErrProfileNotFound).Msgf("User ID for %s is empty, skipping.", username)
		return ErrProfileNotFound
	}
	fl := user.ToFollowingUser()
	var rTag string
	switch strings.ToLower(strings.TrimSpace(tag)) {
	case "trader":
		rTag = twitterprofile.TAG_TRADER.String()
	case "meme":
		rTag = twitterprofile.TAG_MEME.String()
	case "influencer":
		rTag = twitterprofile.TAG_INFLUENCER.String()
	case "company":
		rTag = twitterprofile.TAG_COMPANY.String()
	case "other":
		rTag = twitterprofile.TAG_OTHER.String()
	default:
		rTag = twitterprofile.TAG_TRADER.String()
	}

	r := &twitterprofile.FetchedProfile{
		ID:         fl.ID,
		UserName:   fl.UserName,
		Handle:     fl.Handle,
		ProfileImg: fl.ProfileImg,
		Followers:  fl.Followers,
		Following:  fl.Following,
		Verified:   fl.Verified,
		Tag:        rTag,
		Status:     twitterprofile.NEW_PROFILE, // 0: not tracked, 1: tracking, 2: not tracking
		CapturedAt: time.Now(),
		UpdatedAt:  time.Now(),
	}
	if err := f.twtPrf.UpdateOrInsertFetched(ctx, r); err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to upsert twitter profile.")
		return ErrDatabaseError
	}
	return nil
}

func contains(item string) bool {
	slice := twitterprofile.AllTags()
	for _, v := range slice {
		if v.String() == item {
			return true
		}
	}
	return false
}
