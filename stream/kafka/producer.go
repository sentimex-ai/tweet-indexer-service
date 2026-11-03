package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/jeetdotso/tweet-indexer-service/config"
	"github.com/jeetdotso/tweet-indexer-service/logg"
	"github.com/jeetdotso/tweet-indexer-service/stream"
)

type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds and returns a stream.Producer backed by kafka-go
func NewProducer(cfg config.Stream, topic string) stream.Producer {
	p := &Producer{}
	p.Connect(cfg, topic)
	return p
}

// NewProducerFor is NewProducer behind the features.kafka switch. With Kafka off it
// returns a producer that drops messages (and says so once per call at debug level),
// so services that only optionally publish can be built without any Kafka setup.
func NewProducerFor(cfg config.Config, topic string) stream.Producer {
	if !cfg.Features.Kafka {
		return nopProducer{topic: topic}
	}
	return NewProducer(cfg.Stream, topic)
}

type nopProducer struct{ topic string }

func (n nopProducer) Produce(key []byte, msg []byte) error {
	logg.Log.Debug().Str("topic", n.topic).Msg("kafka is off (features.kafka=false), message dropped")
	return nil
}

// Connect dials your VPC‐peered Kafka (:9092) using TLS only
func (p *Producer) Connect(cfg config.Stream, topic string) {
	logg.Log.Info().
		Msgf("Connecting to Kafka brokers (private, TLS only): %v", cfg.Kafka.Brokers)

	// 1) load client cert + key
	cert, err := tls.LoadX509KeyPair(
		"/etc/kafka/user-access-certificate.crt",
		"/etc/kafka/user-access-key.key",
	)
	if err != nil {
		logg.Log.Fatal().Err(err).Msg("could not load Kafka client cert/key")
	}

	// 2) load & trust the CA
	caPem, err := ioutil.ReadFile("/etc/kafka/ca.pem")
	if err != nil {
		logg.Log.Fatal().Err(err).Msg("could not load Kafka CA cert")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPem) {
		logg.Log.Fatal().Msg("failed to parse CA certificate")
	}

	// 3) assemble tls.Config
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		MinVersion:   tls.VersionTLS12,
	}

	// 4) (optional) if you’re using SASL/PLAIN over SSL, configure the mechanism:
	var mech sasl.Mechanism
	if cfg.Kafka.UseSASL {
		mech = plain.Mechanism{
			Username: cfg.Kafka.Username,
			Password: cfg.Kafka.Password,
		}
	}

	// 5) build a Dialer
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsCfg,
		SASLMechanism: mech,
	}

	// 6) finally construct your writer
	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond,
		Dialer:       dialer,
	})
}

// Produce sends a single message (with optional key) to Kafka
func (p *Producer) Produce(key, msg []byte) error {
	m := kafka.Message{Key: key, Value: msg}
	if err := p.writer.WriteMessages(context.Background(), m); err != nil {
		logg.Log.Error().Err(err).Msg("Failed to produce message to Kafka")
		return err
	}
	return nil
}
