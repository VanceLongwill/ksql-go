package client

// StreamsProperties is a map of property overrides
// https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/server-config/config-reference/
type StreamsProperties map[string]string

// @TODO: Add more option shorthands

// StreamsPropertiesOption configures an option in the StreamsProperties map
type StreamsPropertiesOption func(StreamsProperties)

var (
	// OffsetEarliest configures the query to stream from the beginning
	OffsetEarliest StreamsPropertiesOption = func(s StreamsProperties) {
		s["ksql.streams.auto.offset.reset"] = "earliest"
	}

	// OffsetLatest is the default offset strategy reading from the latest item in the stream
	OffsetLatest StreamsPropertiesOption = func(s StreamsProperties) {
		s["ksql.streams.auto.offset.reset"] = "latest"
	}

	// ExactlyOnce enables exactly-once semantics for the query. If a producer within a ksqlDB application sends a duplicate record, it's written to the broker exactly once.
	ExactlyOnce StreamsPropertiesOption = func(s StreamsProperties) {
		s["processing.guarantee"] = "exactly_once"
	}

	// AtLeastOnce enables at-least-once semantics for the query and is the default setting. Records are never lost but may be redelivered.
	AtLeastOnce StreamsPropertiesOption = func(s StreamsProperties) {
		s["processing.guarantee"] = "at_least_once"
	}
)

// NewStreamsProperties creates a new StreamsProperties map with a optional set of options
func NewStreamsProperties(options ...StreamsPropertiesOption) StreamsProperties {
	s := StreamsProperties{}
	s.With(options...)
	return s
}

// With configures the StreamsProperties map with the provided options
func (s StreamsProperties) With(options ...StreamsPropertiesOption) StreamsProperties {
	for _, opt := range options {
		opt(s)
	}
	return s
}
