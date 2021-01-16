package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandExecAs(t *testing.T) {
	t.Run("given a non-nil CommandResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			CommandResult: &CommandResult{
				CommandID: "some-command",
			},
		}
		var command CommandResult
		assert.True(t, res.As(&command))
		assert.Equal(t, res.commonResult, command.commonResult, "the common result struct should be copied over")
		command.commonResult = commonResult{}
		assert.Equal(t, res.CommandResult, &command, "the result should be copied over")
	})
	t.Run("given a nil CommandResult", func(t *testing.T) {
		res := &ExecResult{}
		var command CommandResult
		assert.False(t, res.As(&command))
	})
}
