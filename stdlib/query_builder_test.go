package stdlib

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	t.Run("returns an error when there's no trailing semi-colon", func(t *testing.T) {
		_, err := buildStatement("SELECT * FROM t1 WHERE name = $1",
			[]driver.NamedValue{{Ordinal: 1, Value: "Bob"}})
		assert.Error(t, err)
		assert.True(t, err == ErrMissingSemicolon)
	})
	t.Run("replaces positional args", func(t *testing.T) {
		t.Run("in the correct order", func(t *testing.T) {
			got, err := buildStatement("SELECT * FROM t1 WHERE name = $1 AND age = $2;", []driver.NamedValue{
				{Ordinal: 1, Value: "Bob"},
				{Ordinal: 2, Value: 45},
			})
			assert.NoError(t, err)
			assert.Equal(t, `SELECT * FROM t1 WHERE name = Bob AND age = 45;`, got)
		})
		t.Run("by their position numbers", func(t *testing.T) {
			got, err := buildStatement("SELECT * FROM t1 WHERE name = $2 AND age = $1;", []driver.NamedValue{
				{Ordinal: 1, Value: 45},
				{Ordinal: 2, Value: "Bob"},
			})
			assert.NoError(t, err)
			assert.Equal(t, `SELECT * FROM t1 WHERE name = Bob AND age = 45;`, got)
		})
	})
	t.Run("replaces named args", func(t *testing.T) {
		t.Run("by their position numbers", func(t *testing.T) {
			got, err := buildStatement("SELECT * FROM t1 WHERE name = :name AND age = :age;", []driver.NamedValue{
				{Ordinal: 1, Value: "Bob", Name: "name"},
				{Ordinal: 2, Value: 45, Name: "age"},
			})
			assert.NoError(t, err)
			assert.Equal(t, `SELECT * FROM t1 WHERE name = Bob AND age = 45;`, got)
		})
	})
}
