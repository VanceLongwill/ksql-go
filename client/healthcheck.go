package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// HealthcheckResult represents the health check information returned by the health check endpoint
type HealthcheckResult struct {
	IsHealthy bool `json:"isHealthy"`
	Details   struct {
		Metastore struct {
			IsHealthy bool `json:"isHealthy"`
		} `json:"metastore"`
		Kafka struct {
			IsHealthy bool `json:"isHealthy"`
		} `json:"kafka"`
	} `json:"details"`
}

// Healthcheck gets basic health information from the ksqlDB cluster
func (c *Client) Healthcheck(ctx context.Context) (HealthcheckResult, error) {
	result := HealthcheckResult{}
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
