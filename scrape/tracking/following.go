package tracking

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dummyaccount "github.com/jeetdotso/tweet-indexer-service/dummyaccount"
	"github.com/jeetdotso/tweet-indexer-service/logg"
	twitterAPI "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api"
	twitterTypes "github.com/jeetdotso/tweet-indexer-service/scrape/internals/api/types"
	followingParsers "github.com/jeetdotso/tweet-indexer-service/scrape/internals/parsers/following"
	"github.com/tidwall/gjson"
)

type FollowingMonitor struct {
	twitterAPI *twitterAPI.TwitterClient
}
type accountJob struct {
	id string
}

type followingJob struct {
	sourceID  string
	following twitterTypes.FollowingUser
}

func NewFollowingMonitor(ctx context.Context, svc dummyaccount.Service) (*FollowingMonitor, *dummyaccount.SelAccount, error) {
	sel, err := svc.LogInToNextGroup(ctx, dummyaccount.WORKING_GROUP)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Error retrieving Selenium account.")
		return nil, nil, err
	}
	client, _ := twitterAPI.CreateTwitterClientWithCookies(sel.GetCookiesHttp(), "")
	return &FollowingMonitor{twitterAPI: client}, sel, nil
}

// ExtractEntries pulls out the array of “user” entries from the GraphQL payload
func (fm *FollowingMonitor) ExtractEntries(r *gjson.Result) []gjson.Result {
	instrs := r.
		Get("data.user.result.timeline.timeline.instructions").
		Array()

	for _, ins := range instrs {
		if ins.Get("type").String() == "TimelineAddEntries" {
			allEntries := ins.Get("entries").Array()
			// strip off the last two, which are the "cursor" entries
			if len(allEntries) >= 2 {
				return allEntries[:len(allEntries)-2]
			}
			return allEntries
		}
	}
	return nil
}

func MonitorSingleFollowingPagePipeline(
	ctx context.Context,
	accountIDs map[string]struct{},
	fetcherCount int,
	handlerCount int,
	maxRetries int,
	retryDelay time.Duration,
	handleFollowing func(ctx context.Context, sourceID string, user twitterTypes.FollowingUser),
	dummSvc dummyaccount.Service,
) {
	type accountJob struct {
		id string
	}

	type followingJob struct {
		sourceID  string
		following twitterTypes.FollowingUser
	}

	userJobs := make(chan accountJob, fetcherCount*2)
	followingJobs := make(chan followingJob, handlerCount*2)

	var wgFetchers sync.WaitGroup
	var wgHandlers sync.WaitGroup

	// Worker pool
	wgFetchers.Add(fetcherCount)
	for i := 0; i < fetcherCount; i++ {
		go func() {
			defer wgFetchers.Done()
			for job := range userJobs {
				fm, selAcc, err := NewFollowingMonitor(ctx, dummSvc)
				if err != nil {
					logg.Log.Error().
						Caller().
						Ctx(ctx).
						Err(err).
						Str("id", job.id).
						Msg("FollowingMonitor init failed")
					continue
				}

				// Single fetch attempt
				resp, err := fetchWithRetries(ctx, fm, job.id, "", 1, maxRetries, retryDelay)
				if err != nil {
					// Cleanup and continue to next job
					if err := dummSvc.LogOut(ctx, selAcc.UserName); err != nil {
						logg.Log.Error().
							Err(err).
							Str("username", selAcc.UserName).
							Str("source_id", job.id).
							Msg("logout failed")
					}
					continue
				}

				// Process entries
				entries, _ := extractEntriesAndCursor(resp)
				for _, e := range entries {
					u := followingParsers.ParseFollowing(&e)
					if u == nil || u.Handle == "" {
						logg.Log.Error().
							Str("id", job.id).
							Msgf("failed to parse entry %s", e.Raw)
						continue
					}
					followingJobs <- followingJob{
						sourceID:  job.id,
						following: *u,
					}
				}

				// Cleanup
				if err := dummSvc.LogOut(ctx, selAcc.UserName); err != nil {
					logg.Log.Error().
						Err(err).
						Str("username", selAcc.UserName).
						Str("source_id", job.id).
						Msg("logout failed")
				}
			}
		}()
	}

	// Handler pool
	wgHandlers.Add(handlerCount)
	for i := 0; i < handlerCount; i++ {
		go func() {
			defer wgHandlers.Done()
			for job := range followingJobs {
				handleFollowing(ctx, job.sourceID, job.following)
			}
		}()
	}

	// Producer
	go func() {
		for id := range accountIDs {
			userJobs <- accountJob{id: id}
		}
		close(userJobs)
	}()

	// Shutdown coordination
	go func() {
		wgFetchers.Wait()
		close(followingJobs)
	}()

	wgHandlers.Wait()
}

func MonitorFollowingsPipeline(
	ctx context.Context,
	accountIDs map[string]struct{},
	fetcherCount int,
	handlerCount int,
	maxRetries int,
	retryDelay time.Duration,
	handleFollowing func(ctx context.Context, sourceID string, user twitterTypes.FollowingUser),
	dummSvc dummyaccount.Service,
) {

	// Buffered channels for better throughput
	accountJobs := make(chan accountJob, fetcherCount*2)
	followingJobs := make(chan followingJob, handlerCount*2)

	var wgFetchers sync.WaitGroup
	var wgHandlers sync.WaitGroup

	// —— 1) Fetcher worker pool ——
	wgFetchers.Add(fetcherCount)
	for i := 0; i < fetcherCount; i++ {
		go func() {
			defer wgFetchers.Done()

			for job := range accountJobs {
				processAccount(
					ctx,
					job,
					accountJobs,
					followingJobs,
					maxRetries,
					retryDelay,
					dummSvc,
				)
			}
		}()
	}

	// —— 2) Handler worker pool ——
	wgHandlers.Add(handlerCount)
	for i := 0; i < handlerCount; i++ {
		go func() {
			defer wgHandlers.Done()
			for job := range followingJobs {
				handleFollowing(ctx, job.sourceID, job.following)
			}
		}()
	}

	// —— 3) Producer ——
	go func() {
		defer close(accountJobs)
		for id := range accountIDs {
			select {
			case accountJobs <- accountJob{id: id}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// —— 4) Shutdown coordination ——
	go func() {
		wgFetchers.Wait()
		close(followingJobs)
	}()

	// —— 5) Wait for completion ——
	wgHandlers.Wait()
}

func processAccount(
	ctx context.Context,
	job accountJob,
	accountJobs chan<- accountJob,
	followingJobs chan<- followingJob,
	maxRetries int,
	retryDelay time.Duration,
	dummSvc dummyaccount.Service,
) {
	cursor := ""
	batch := 1

	fm, selAcc, err := NewFollowingMonitor(ctx, dummSvc)
	if err != nil {
		logg.Log.Error().
			Caller().
			Ctx(ctx).
			Err(err).
			Str("id", job.id).
			Msg("FollowingMonitor creation failed")
		return
	}
	defer cleanupAccount(ctx, dummSvc, selAcc.UserName, job.id)

	for {
		// Fetch with retries
		resp, err := fetchWithRetries(ctx, fm, job.id, cursor, batch, maxRetries, retryDelay)
		if err != nil {
			return
		}

		// Process entries
		entries, next := extractEntriesAndCursor(resp)
		if len(entries) == 0 {
			logg.Log.Info().Str("id", job.id).Msgf("empty page %d", batch)
			return
		}

		if !processEntries(ctx, job, entries, followingJobs) {
			return
		}

		// Check if we should continue
		if isEndOfList(next) {
			logg.Log.Info().Str("id", job.id).Msgf("completed after %d batches", batch)
			return
		}

		cursor = next
		batch++
	}
}

func cleanupAccount(ctx context.Context, dummSvc dummyaccount.Service, username, accountID string) {
	if err := dummSvc.LogOut(ctx, username); err != nil {
		logg.Log.Error().
			Err(err).
			Str("username", username).
			Str("account_id", accountID).
			Msg("logout failed")
	}
}

func fetchWithRetries(
	ctx context.Context,
	fm *FollowingMonitor,
	accountID string,
	cursor string,
	batch int,
	maxRetries int,
	retryDelay time.Duration,
) (*gjson.Result, error) {
	var resp *gjson.Result
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			resp, err = fm.twitterAPI.Following(accountID, cursor)
			if err == nil {
				return resp, nil
			}

			logg.Log.Error().
				Caller().
				Ctx(ctx).
				Err(err).
				Str("id", accountID).
				Msgf("fetching following page %d attempt %d/%d failed", batch, attempt, maxRetries)

			if attempt < maxRetries {
				select {
				case <-time.After(retryDelay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
	}

	logg.Log.Error().
		Str("id", accountID).
		Msgf("max retries reached for page %d", batch)
	return nil, fmt.Errorf("max retries reached for account %s page %d", accountID, batch)
}

func processEntries(
	ctx context.Context,
	job accountJob,
	entries []gjson.Result,
	followingJobs chan<- followingJob,
) bool {
	for _, e := range entries {
		u := followingParsers.ParseFollowing(&e)
		if u == nil || u.Handle == "" {
			logg.Log.Error().
				Str("id", job.id).
				Msgf("failed to parse entry %s", e.Raw)
			continue
		}

		select {
		case followingJobs <- followingJob{
			sourceID:  job.id,
			following: *u,
		}:
		case <-ctx.Done():
			logg.Log.Info().
				Str("id", job.id).
				Msg("context cancelled while processing entries")
			return false
		}
	}
	return true
}

func isEndOfList(nextCursor string) bool {
	return followingParsers.IsEnd(nextCursor)
}

// MonitorSingleAccount fetches *all* pages of “following” for the given user,
// parses every entry into FollowingUser, and invokes handleFollowing on each.
func (fm *FollowingMonitor) MonitorSingleAccount(
	ctx context.Context,
	id string,
	maxRetries int,
	retryDelay time.Duration,
	handleFollowing func(context.Context, string, twitterTypes.FollowingUser),
) error {
	// 1) look up userID (cache or however you store it)

	cursor := ""
	batch := 1

	for {
		// 2) fetch page with retries
		var resp *gjson.Result
		for attempt := 1; attempt <= maxRetries; attempt++ {
			r, err := fm.twitterAPI.Following(id, cursor)
			if err == nil {
				resp = r
				break
			}
			logg.Log.Error().Caller().Ctx(ctx).Err(err).Msgf("id %s: fetching following page %d attempt %d/%d failed", id, batch, attempt, maxRetries)
			time.Sleep(retryDelay)
		}
		if resp == nil {
			return fmt.Errorf("max retries reached for %s page %d", id, batch)
		}

		// 3) extract entries
		entries, next := extractEntriesAndCursor(resp)
		if len(entries) == 0 {
			logg.Log.Info().Caller().Ctx(ctx).
				Msgf("id %q: no entries on page %d → stopping", id, batch)
			break
		}
		for _, e := range entries {
			u := followingParsers.ParseFollowing(&e)
			if u == nil {
				logg.Log.Error().Caller().Ctx(ctx).
					Msgf("id %q: failed to parse entry %s", id, e.Raw)
				continue
			}
			if u.Handle == "" {
				logg.Log.Error().Caller().Ctx(ctx).
					Msgf("id %q: empty handle for entry %s", id, e.Raw)
				continue
			}
			handleFollowing(ctx, id, *u)
		}

		// if Twitter returns the “end” sentinel, we’re done
		if next == "" || strings.HasPrefix(next, "0|") {
			logg.Log.Info().Caller().Ctx(ctx).
				Msgf("id %q: end of list reached on batch %d", id, batch)
			break
		}

		// otherwise, use it for the next round
		cursor = next
		batch++
	}

	return nil
}

// extractEntriesAndCursor returns the user entries of one page and the cursor
// for the next one. See followingParsers.ExtractPage for how entries are told apart.
func extractEntriesAndCursor(r *gjson.Result) (users []gjson.Result, nextCursor string) {
	return followingParsers.ExtractPage(r)
}
