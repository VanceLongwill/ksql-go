package ksql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/ksql"
)

func TestQueryConfig(t *testing.T) {
	t.Run("it should set stream", func(t *testing.T) {
		q := ksql.NewQueryConfig().Stream()
		assert.Equal(t, q.Strategy, ksql.StreamQuery)
	})
	t.Run("it should handle property options correctly", func(t *testing.T) {
		q := ksql.NewQueryConfig().WithProperties(ksql.OffsetLatest)
		assert.Equal(t, q.StreamsProperties["ksql.streams.auto.offset.reset"], "latest")
	})
}
