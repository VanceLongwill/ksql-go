package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// InfoResult is a map of status information
type InfoResult map[string]interface{}

// Info returns status information about the ksqlDB cluster
func (c *Client) Info(ctx context.Context) (InfoResult, error) {
	result := InfoResult{}
	req, err := makeRequest(ctx, c.baseURL, infoPath, http.MethodGet, nil)
	if err != nil {
		return result, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}
