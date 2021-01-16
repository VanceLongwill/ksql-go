package client

import (
	"context"
	"fmt"
)

type DescribeResult struct {
	commonResult
	// SourceDescription is a detailed description of the source (a STREAM or TABLE)
	SourceDescription SourceDescription `json:"sourceDescription,omitempty"`
}

func (d *DescribeResult) Is(target ExecResult) bool {
	if target.DescribeResult != nil {
		*d = *target.DescribeResult
		d.commonResult = target.commonResult
		return true
	}
	return false
}

func (c *ksqldb) Describe(ctx context.Context, source string) (DescribeResult, error) {
	var describe DescribeResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: fmt.Sprintf("DESCRIBE %s;", source)})
	if err != nil {
		return describe, err
	}
	_ = res.As(&describe)
	return describe, nil
}
