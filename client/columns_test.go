package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	t.Run("given a column count is NOT -1", func(t *testing.T) {
		testCases := []columns{
			{},
			{count: 3},
		}
		for _, c := range testCases {
			t.Run("when Validate is called with an incorrect number of columns", func(t *testing.T) {
				got := make([]interface{}, c.count+1)
				err := c.Validate(got)
				assert.Error(t, err)
				assert.Equal(t, ErrColumnNumberMismatch, err)
			})
			t.Run("when Validate is called with the correct number of columns", func(t *testing.T) {
				got := make([]interface{}, c.count)
				err := c.Validate(got)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("when the column count is explicitly unset", func(t *testing.T) {
		c := columns{count: unset}
		t.Run("when Validate is called", func(t *testing.T) {
			err := c.Validate([]interface{}{
				nil,
				nil,
			})
			assert.NoError(t, err, "an error should never be returned")
		})
	})

	t.Run("given the column names are provided", func(t *testing.T) {
		names := []string{"a", "b", "c"}
		t.Run("when the column count matches the number of names", func(t *testing.T) {
			c := columns{count: len(names), names: names}
			assert.Equal(t, c.Columns(), names)
		})
		t.Run("when the column count does not match the number of names", func(t *testing.T) {
			c := columns{count: len(names) - 1, names: names}
			assert.Equal(t, c.Columns(), make([]string, c.count))
		})
		t.Run("when the column count is unset", func(t *testing.T) {
			c := columns{count: len(names), names: names}
			assert.Equal(t, c.Columns(), names)
		})
	})
}
