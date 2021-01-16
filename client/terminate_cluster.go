package ksql

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
)

// TerminateClusterPayload represents the request body payload to terminate a ksqlDB cluster
type TerminateClusterPayload struct {
	// DeleteTopicList is an optional list of Kafka topics to delete
	DeleteTopicList []string `json:"deleteTopicList,omitempty"`
}

// TerminateCluster terminates a running ksqlDB cluster
func (c *Client) TerminateCluster(ctx context.Context, payload TerminateClusterPayload) error {
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(&payload); err != nil {
		return err
	}
	req, err := makeRequest(ctx, c.baseURL, terminateClusterPath, http.MethodPost, b)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
