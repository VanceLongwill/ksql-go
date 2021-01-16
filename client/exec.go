package client

import (
	"bytes"
	"context"
	"encoding/json"
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

	// CommandID is the identified for the requested operation. You can use this ID to poll the result of the operation using the status endpoint.
	CommandID string `json:"commandId,omitempty"`
	// CommandStatus is the status of the requested operation.
	CommandStatus CommandStatus `json:"commandStatus,omitempty"`

	// LIST STREAMS, SHOW STREAMS

	// Streams is the list of streams returned
	Streams []Stream `json:"streams,omitempty"`

	// LIST TABLES, SHOW TABLES

	ListTablesResult

	// LIST QUERIES, SHOW QUERIES

	// Queries is the list of queries started
	Queries []Query `json:"queries,omitempty"`

	// LIST PROPERTIES, SHOW PROPERTIES

	// Properties is the map of server query properties
	Properties map[string]string `json:"properties,omitempty"`

	// DESCRIBE

	// SourceDescription is a detailed description of the source (a STREAM or TABLE)
	SourceDescription SourceDescription `json:"sourceDescription,omitempty"`

	// EXPLAIN

	// QueryDescription is a detailed description of a query statement.
	QueryDescription QueryDescription `json:"queryDescription,omitempty"`
	// OverriddenProperties is a map of property overrides that the query is running with.
	OverriddenProperties map[string]interface{} `json:"overriddenProperties,omitempty"`
}

// Exec runs KSQL statements which can be anything except SELECT
func (c *Client) Exec(ctx context.Context, payload ExecPayload) ([]ExecResult, error) {
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
