package client

// Warning represents a non-fatal user warning
type Warning struct {
	Message string `json:"message"`
}

// Common to all exec responses
type commonResult struct {
	// StatementText is the text of the SQL statement where the error occurred
	StatementText string `json:"statementText"`
	// A list of non-fatal warning messages
	Warnings []Warning `json:"warnings"`

	statementType string
}

// Query is info about a query
type Query struct {
	// QueryString is the text of the statement that started the query
	QueryString string `json:"queryString"`
	// Sinks are the streams and tables being written to by the query
	Sinks []string `json:"sinks"`
	// ID is the query id
	ID string `json:"id"`
}

// Schema represents a ksqlDB fields schema
type Schema struct {
	// The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
	Type string `json:"type"`
	// A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
	MemberSchema map[string]interface{} `json:"memberSchema,omitempty"`
	// For STRUCT types, contains a list of field objects that describes each field within the struct. For other types this field is not used and its value is undefined.
	Fields []Field `json:"fields,omitempty"`
}

// Field represents a single fields in ksqlDB
type Field struct {
	// The name of the field.
	Name string `json:"name"`
	// A schema object that describes the schema of the field.
	Schema Schema `json:"schema"`
}

// SourceDescription is a detailed description of the source (a STREAM or TABLE)
type SourceDescription struct {
	// Name of the stream or table.
	Name string `json:"name"`
	// ReadQueries is the list of queries reading from the stream or table.
	ReadQueries []Query `json:"readQueries"`
	// WriteQueries is the list of queries writing into the stream or table
	WriteQueries []Query `json:"writeQueries"`
	// Fields is a list of field objects that describes each field in the stream/table.
	Fields []Field `json:"fields"`
	// Type is either STREAM or TABLE.
	Type string `json:"type"`
	// Key is the name of the key column.
	Key string `json:"key"`
	// Timestamp is the name of the timestamp column.
	Timestamp string `json:"timestamp"`
	// Format is the serialization format of the data in the stream or table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
	Format string `json:"format"`
	// Topic backing the stream or table.
	Topic string `json:"topic"`
	// Extended indicates if this is an extended description.
	Extended bool `json:"extended"`
	// Statistics about production and consumption to and from the backing topic (extended only).
	Statistics string `json:"statistics,omitempty"`
	// ErrorStats is a string about errors producing and consuming to and from the backing topic (extended only).
	ErrorStats string `json:"errorStats,omitempty"`
	// Replication factor of the backing topic (extended only).
	Replication int `json:"replication,omitempty"`
	// Partitions is the number of partitions in the backing topic (extended only).
	Partitions int `json:"partitions,omitempty"`
}

// QueryDescription is a detailed description of a query statement.
type QueryDescription struct {
	// StatementText is a ksqlDB statement for which the query being explained is running.
	StatementText string `json:"statementText"`
	// Fields is a list of field objects that describes each field in the query output.
	Fields []Field `json:"fields"`
	// Sources is a list of the stream and table names being read by the query.
	Sources []string `json:"sources"`
	// Sinks is a list of the stream and table names being written to by the query.
	Sinks []string `json:"sinks"`
	// ExecutionPlan is the query execution plan.
	ExecutionPlan string `json:"executionPlan"`
	// Topology is the Kafka Streams topology for the query that is running.
	Topology string `json:"topology"`
}
