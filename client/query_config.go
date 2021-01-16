package ksql

// QueryStrategy is used to
type QueryStrategy string

const (
	// StreamQuery uses the clients QueryStream method
	StreamQuery QueryStrategy = "StreamQuery"
	// StaticQuery uses the clients more limited Query method
	StaticQuery = "StaticQuery"
)

// QueryConfig is for use with database/sql based queries
type QueryConfig struct {
	Strategy          QueryStrategy
	StreamsProperties StreamsProperties
}

// DefaultQueryConfig is the fallback config when querying through the database/sql interface
var DefaultQueryConfig = NewQueryConfig()

// NewQueryConfig constructs a new default QueryConfig builder
func NewQueryConfig() *QueryConfig {
	q := new(QueryConfig)
	q.StreamsProperties = StreamsProperties{}
	q = q.Static()
	return q
}

// Stream configures the query to use the /query-stream resource (i.e. pull queries). Results are streamed back until explicitly closed or the context is cancelled.
func (q *QueryConfig) Stream() *QueryConfig {
	q.Strategy = StreamQuery
	return q
}

// Static configures the query to use the /query resource. Queries made with this strategy do not require manual closing.
func (q *QueryConfig) Static() *QueryConfig {
	q.Strategy = StaticQuery
	return q
}

// SetProperties sets the stream properties for the query, replacing any existing properties
func (q *QueryConfig) SetProperties(props StreamsProperties) *QueryConfig {
	q.StreamsProperties = props
	return q
}

// WithProperties is a shorthand for configuring stream options
func (q *QueryConfig) WithProperties(options ...StreamsPropertiesOption) *QueryConfig {
	q.StreamsProperties.With(options...)
	return q
}
