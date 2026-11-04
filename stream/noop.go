package stream

// NoopProducer drops every message. Used when Kafka output is disabled.
type NoopProducer struct{}

func (NoopProducer) Produce(key []byte, msg []byte) error { return nil }
