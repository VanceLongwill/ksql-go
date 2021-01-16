package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExplainExecAs(t *testing.T) {
	t.Run("given a non-nil ExplainResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			ExplainResult: &ExplainResult{
				QueryDescription: QueryDescription{
					StatementText: "yaass",
				},
			},
		}
		var explain ExplainResult
		assert.True(t, res.As(&explain))
		assert.Equal(t, res.commonResult, explain.commonResult, "the common result struct should be copied over")
		explain.commonResult = commonResult{}
		assert.Equal(t, res.ExplainResult, &explain, "the explain result should be copied over")
	})
	t.Run("given a nil ExplainResult", func(t *testing.T) {
		res := &ExecResult{}
		var explain ExplainResult
		assert.False(t, res.As(&explain))
	})
}
