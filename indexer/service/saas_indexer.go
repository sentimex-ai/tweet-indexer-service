package service

import (
	"context"
	"runtime/debug"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/indexer/model"
	"github.com/jeetdotso/tweet-indexer-service/indexer/repository"
	"github.com/jeetdotso/tweet-indexer-service/logg"
	"github.com/jeetdotso/tweet-indexer-service/persistence/mongodb"
	"github.com/jeetdotso/tweet-indexer-service/stream"
	"github.com/jeetdotso/tweet-indexer-service/stream/kafka"
	"google.golang.org/protobuf/proto"
)

type SaasIndexer struct {
	grokRepo                repository.GrokEventRepository
	tweetRepo               repository.TweetEventRepository
	memeProducer            stream.Producer
	traderProducer          stream.Producer
	rawRepo                 repository.RawTweetRepository
	REVIEW_TOPIC_CON_MEME   string
	REVIEW_TOPIC_CON_TRADER string
}

const (
	MEME_TOPIC_NAME   = "shill"
	TRADER_TOPIC_NAME = "trader"
)

func NewSaasIndexer(cfg config.Config, conn *mongodb.MongoConnection) SaasIndexer {
	grokRepo := repository.NewGrokIndexRepo(conn)
	tweetRepo := repository.NewTweetIndexRepo(conn)
	rawRepo := repository.NewRawTweetRepo(conn)

	memeProducer := kafka.NewProducerFor(cfg, cfg.Stream.Kafka.MemeTopic)
	traderProducer := kafka.NewProducerFor(cfg, cfg.Stream.Kafka.TraderTopic)
	return SaasIndexer{
		grokRepo:                grokRepo,
		tweetRepo:               tweetRepo,
		memeProducer:            memeProducer,
		traderProducer:          traderProducer,
		rawRepo:                 rawRepo,
		REVIEW_TOPIC_CON_MEME:   cfg.Stream.Kafka.MemeTopic,
		REVIEW_TOPIC_CON_TRADER: cfg.Stream.Kafka.TraderTopic,
	}
}

func (si *SaasIndexer) TopicNames() []string {
	return []string{si.REVIEW_TOPIC_CON_MEME, si.REVIEW_TOPIC_CON_TRADER}
}

func (si *SaasIndexer) Start(ctx context.Context, grokEvent *model.GrokEvent) {
	// Check if the user is already in the database
	tweet, err := si.tweetRepo.FindByTweetId(ctx, grokEvent.ID)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to fetch tweet event by ID.")
		return
	}
	if tweet == nil {
		logg.Log.Warn().Caller().Ctx(ctx).Str("tweetID", grokEvent.ID).Msg("Tweet event does not exists.")
		return
	}

	saasEvent := model.SaasEvent{
		ID:            grokEvent.ID,
		TweetId:       tweet.TweetID,
		Text:          tweet.Text,
		CreatedAt:     tweet.CreatedAt,
		UserId:        tweet.UserID,
		TweetType:     string(tweet.TweetType),
		TweetUrl:      tweet.GetURL(),
		UserName:      tweet.UserName,
		Category:      tweet.Category,
		TokensShilled: grokEvent.TokensShilled,
	}

	go si.streamTweet(ctx, saasEvent)
}
func (si *SaasIndexer) StartTweet(ctx context.Context, gr *model.GrokEvent, tweetEvent *model.TweetEvent) {
	// Check if the user is already in the database
	var tokensShilled []string
	if gr.IsShill {
		tokensShilled = gr.TokensShilled
	} else {
		return
	}
	saasEvent := model.SaasEvent{
		ID:            tweetEvent.ID,
		TweetId:       tweetEvent.TweetID,
		Text:          tweetEvent.Text,
		CreatedAt:     tweetEvent.CreatedAt,
		UserId:        tweetEvent.UserID,
		TweetType:     string(tweetEvent.TweetType),
		UserName:      tweetEvent.UserName,
		Category:      tweetEvent.Category,
		TweetUrl:      tweetEvent.GetURL(),
		TokensShilled: tokensShilled,
	}
	// if !gr.IsShill {

	// 	log.Printf("→ publishing event: isShill=%v tokensShilled=%v",
	// 		gr.IsShill, saasEvent.TokensShilled,
	// 	)
	// }

	go si.streamTweet(ctx, saasEvent)
}

func (si *SaasIndexer) streamTweet(ctx context.Context, saasEvt model.SaasEvent) {
	// catch any panics inside the goroutine
	defer func() {
		if r := recover(); r != nil {
			logg.Log.Error().
				Caller().
				Ctx(ctx).
				Msgf("Recovered from panic in GetGrokCheck for tweet %s: %v\n%s",
					saasEvt.ID, r, debug.Stack(),
				)
		}
	}()

	saasProto := saasEvt.ToProto()
	b, err := proto.Marshal(saasProto)
	if err != nil {
		logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to marshal proto message.")
		return
	}
	switch saasEvt.Category {
	case model.CategoryTypeMEME:
		// Produce message to Kafka
		if err = si.memeProducer.Produce([]byte(LIVE_KEY), b); err != nil {
			logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to produce stream message.")
			return
		}
		logg.Log.Info().Caller().Ctx(ctx).Str("tweetId", saasEvt.ID).Str("topic", si.REVIEW_TOPIC_CON_MEME).Msg("Saas Tweet event sent!")
	case model.CategoryTypeTRADER:
		// Produce message to Kafka
		if err = si.traderProducer.Produce([]byte(LIVE_KEY), b); err != nil {
			logg.Log.Error().Caller().Ctx(ctx).Err(err).Msg("Failed to produce stream message.")
			return
		}
		logg.Log.Info().Caller().Ctx(ctx).Str("tweetId", saasEvt.ID).Str("topic", si.REVIEW_TOPIC_CON_TRADER).Msg("Saas Tweet event sent!")
	default:
		return

	}

}
