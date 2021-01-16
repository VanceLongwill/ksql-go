package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

// CommandStatus contains details of status of a given command
type CommandStatus struct {
	// Status is one of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR
	Status string `json:"status"`
	// Message regarding the status of the execution statement.
	Message string `json:"message"`
	// CommandSequenceNumber is the sequence number of the command, -1 if unsuccessful
	CommandSequenceNumber int64 `json:"commandSequenceNumber"`
}

// ExecPayload represents the JSON payload for the /ksql endpoint
type ExecPayload struct {
	// KSQL is a sequence of SQL statements. Anything is permitted except SELECT, for which you should use the Query method
	KSQL string `json:"ksql"`
	// StreamsProperties is a map of property overrides
	StreamsProperties StreamsProperties `json:"streamsProperties,omitempty"`
	// CommandSequenceNumber optionally waits until the specified sequence has been completed before running
	CommandSequenceNumber int64 `json:"commandSequenceNumber,omitempty"`
}

// ExecResult is the response result from the /ksql endpoint
type ExecResult struct {
	commonResult

	// CREATE, DROP, TERMINATE
	*CommandResult

	// LIST STREAMS, SHOW STREAMS
	*ListStreamsResult

	// LIST TABLES, SHOW TABLES
	*ListTablesResult

	// LIST QUERIES, SHOW QUERIES
	*ListQueriesResult

	// LIST PROPERTIES, SHOW PROPERTIES
	*ListPropertiesResult

	// DESCRIBE
	*DescribeResult

	// EXPLAIN
	*ExplainResult
}

type Result interface {
	Is(target ExecResult) bool
}

// As checks if the ExecResult contains a subset result.
// If it does, then data is copied over for convenience.
func (e ExecResult) As(target Result) bool {
	if t, ok := target.(Result); ok {
		return t.Is(e)
	}
	return false
}

// Exec runs KSQL statements which can be anything except SELECT
func (c *ksqldb) Exec(ctx context.Context, payload ExecPayload) ([]ExecResult, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := makeRequest(ctx, c.baseURL, execPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to make Exec request: %w", err)
	}
	var results []ExecResult
	by, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body: %w", err)
	}
	if err := json.Unmarshal(by, &results); err != nil {
		var result ExecResult
		if err := json.Unmarshal(by, &result); err != nil {
			return nil, fmt.Errorf("unable to decode JSON response '%s': %w", string(by), err)
		}
		results = append(results, result)
	}
	return results, nil
}

func (c *ksqldb) singleExec(ctx context.Context, payload ExecPayload) (ExecResult, error) {
	var resp ExecResult
	results, err := c.Exec(ctx, payload)
	if err != nil {
		return resp, err
	}
	if len(results) == 0 {
		return resp, errors.New("Unexpected empty response list from Exec")
	}
	if len(results) > 1 {
		return resp, errors.New("Expected only 1 result from Exec")
	}
	return results[0], nil
}
