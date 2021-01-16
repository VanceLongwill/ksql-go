package client

import (
	"context"
	"fmt"
)

type ExplainResult struct {
	commonResult

	// QueryDescription is a detailed description of a query statement.
	QueryDescription QueryDescription `json:"queryDescription,omitempty"`
	// OverriddenProperties is a map of property overrides that the query is running with.
	OverriddenProperties map[string]interface{} `json:"overriddenProperties,omitempty"`
}

func (e *ExplainResult) is(target ExecResult) bool {
	if target.ExplainResult != nil {
		*e = *target.ExplainResult
		e.commonResult = target.commonResult
		return true
	}
	return false
}

func (c *ksqldb) Explain(ctx context.Context, queryNameOrExpression string) (ExplainResult, error) {
	var e ExplainResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: fmt.Sprintf("EXPLAIN %s;", queryNameOrExpression)})
	if err != nil {
		return e, err
	}
	_ = res.As(&e)
	return e, nil
}
