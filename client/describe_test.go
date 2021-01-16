package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDescribeExecAs(t *testing.T) {
	t.Run("given a non-nil DescribeResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			DescribeResult: &DescribeResult{
				SourceDescription: SourceDescription{
					Name: "something",
				},
			},
		}
		var describe DescribeResult
		assert.True(t, res.As(&describe))
		assert.Equal(t, res.commonResult, describe.commonResult, "the common result struct should be copied over")
		describe.commonResult = commonResult{}
		assert.Equal(t, res.DescribeResult, &describe, "the describe result should be copied over")
	})
	t.Run("given a nil DescribeResult", func(t *testing.T) {
		res := &ExecResult{}
		var describe DescribeResult
		assert.False(t, res.As(&describe))
	})
}
