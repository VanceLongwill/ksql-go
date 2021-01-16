package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func parseSchemaKeys(str string) []string {
	var keys []string
	b := strings.Builder{}
	write := false
	for _, r := range str {
		if r == '`' {
			write = !write
			if !write {
				keys = append(keys, b.String())
				b.Reset()
			}
		} else if write {
			b.WriteRune(r)
		}

	}
	return keys
}

// QueryPayload represents the JSON payload for the POST /query endpoint
type QueryPayload struct {
	// KSQL is SELECT statement
	KSQL string `json:"ksql"`
	// StreamsProperties is a map of property overrides
	StreamsProperties StreamsProperties `json:"streamsProperties,omitempty"`
}

// Row is a row in the DB
type Row struct {
	Columns []interface{} `json:"columns"`
}

// QueryResult is the result of running a query
type QueryResult struct {
	Row          Row    `json:"row"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	FinalMessage string `json:"finalMessage,omitempty"`
}

// QueryError represents an error querying
type QueryError struct {
	result map[string]interface{}
}

func (q *QueryError) Error() string {
	if msg, ok := q.result["message"]; ok {
		return msg.(string)
	}
	return "an unknown error occurred"
}

// Query runs a KSQL query and returns a cursor. For streaming results use the QueryStream method.
func (c *ksqldb) Query(ctx context.Context, payload QueryPayload) (*QueryRows, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := makeRequest(ctx, c.baseURL, queryPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get response: %w", err)
	}
	defer resp.Body.Close()
	by, err := ioutil.ReadAll(resp.Body)
	var statementError map[string]interface{}
	if err := json.Unmarshal(by, &statementError); err == nil {
		return nil, &QueryError{statementError}
	}
	var resultsRaw []map[string]interface{}
	if err := json.Unmarshal(by, &resultsRaw); err != nil {
		return nil, err
	}
	cols := columns{
		count: -1,
	}
	if h, ok := resultsRaw[0]["header"]; ok {
		if headerMap, ok := h.(map[string]interface{}); ok {
			if schema, exists := headerMap["schema"]; exists {
				cols.names = parseSchemaKeys(schema.(string))
				cols.count = len(cols.names)
			}
		}
	}
	return &QueryRows{
		res:     resultsRaw[1:],
		columns: cols,
	}, nil
}
