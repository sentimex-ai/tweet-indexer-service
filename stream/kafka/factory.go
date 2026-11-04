package kafka

import (
	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/stream"
)

// NewProducerIfEnabled returns a real Kafka producer only when
// stream.kafka.enabled is true. Otherwise it returns a no-op producer, so
// code paths that used to publish to Kafka keep working without brokers or
// the /etc/kafka client certificates (NewProducer exits the process when
// those files are missing).
func NewProducerIfEnabled(cfg config.Stream, topic string) stream.Producer {
	if !cfg.Kafka.Enabled {
		return stream.NoopProducer{}
	}
	return NewProducer(cfg, topic)
}
