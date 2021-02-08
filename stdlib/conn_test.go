package stdlib

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	ksql "github.com/vancelongwill/ksql/client"
	"github.com/vancelongwill/ksql/stdlib/mocks"
)

func TestConn(t *testing.T) {
	t.Run("PrepareContext", func(t *testing.T) {
		c := newConn(nil)
		query := "some query"
		stmt, err := c.PrepareContext(context.Background(), query)
		assert.NoError(t, err)
		assert.Equal(t, 1, c.stmtNameCounter)
		preparedStmt := (stmt).(PreparedStatement)
		assert.Equal(t, query, preparedStmt.SQL)
		assert.Equal(t, "1", preparedStmt.Name)
	})

	t.Run("CheckNamedValue", func(t *testing.T) {
		t.Run("when the value is a QueryConfig", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			nv := &driver.NamedValue{
				Name:  "",
				Value: ksql.QueryConfig{},
			}
			err := c.CheckNamedValue(nv)
			assert.Equal(t, driver.ErrRemoveArgument, err)
		})
		t.Run("when the value is NOT a QueryConfig", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			nv := &driver.NamedValue{
				Name:  "",
				Value: "something",
			}
			err := c.CheckNamedValue(nv)
			assert.NoError(t, err)
		})
	})

	t.Run("QueryContext", func(t *testing.T) {
		t.Run("when a query strategy is not specified", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			query := "select something from somewhere where prop = $1;"
			nv := []driver.NamedValue{
				{
					Name:  "",
					Value: 1,
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().
				Query(ctx, ksql.QueryPayload{
					KSQL:              query,
					StreamsProperties: ksql.StreamsProperties{},
				}).
				Return(nil, nil)
			_, err := c.QueryContext(ctx, query, nv)
			assert.NoError(t, err)
		})

		t.Run("when a the stream strategy is specified", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			query := "select something from somewhere where prop = $1;"
			nv := []driver.NamedValue{
				{
					Name:  "",
					Value: 1,
				},
				{
					Name: "",
					Value: &ksql.QueryConfig{
						Strategy: ksql.StreamQuery,
					},
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().
				QueryStream(ctx, ksql.QueryStreamPayload{
					KSQL:       query,
					Properties: nil,
				}).
				Return(nil, nil)
			_, err := c.QueryContext(ctx, query, nv)
			assert.NoError(t, err)
		})

		t.Run("when a the static strategy is specified", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			query := "select something from somewhere where prop = $1;"
			nv := []driver.NamedValue{
				{
					Name:  "",
					Value: 1,
				},
				{
					Name: "",
					Value: &ksql.QueryConfig{
						Strategy: ksql.StaticQuery,
					},
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().
				Query(ctx, ksql.QueryPayload{
					KSQL:              query,
					StreamsProperties: nil,
				}).
				Return(nil, nil)
			_, err := c.QueryContext(ctx, query, nv)
			assert.NoError(t, err)
		})
	})

	t.Run("ExecContext", func(t *testing.T) {
		t.Run("when given a statement", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			query := "UPDATE sometable set SOMETHING = $1;"
			nv := []driver.NamedValue{
				{
					Name:    "",
					Value:   1,
					Ordinal: 1,
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().
				Exec(ctx, ksql.ExecPayload{
					KSQL:              "UPDATE sometable set SOMETHING = 1;",
					StreamsProperties: nil,
				}).
				Return(nil, nil)
			_, err := c.ExecContext(ctx, query, nv)
			assert.NoError(t, err)
		})
		t.Run("when StreamsProperties is passed as the final arg", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClient := mocks.NewMockClient(ctrl)
			c := newConn(mockClient)
			query := "UPDATE sometable set SOMETHING = $1;"
			props := ksql.NewStreamsProperties(ksql.OffsetEarliest)
			nv := []driver.NamedValue{
				{
					Name:    "",
					Value:   1,
					Ordinal: 1,
				},
				{
					Name:  "",
					Value: props,
				},
			}
			ctx := context.Background()
			mockClient.EXPECT().
				Exec(ctx, ksql.ExecPayload{
					KSQL:              "UPDATE sometable set SOMETHING = 1;",
					StreamsProperties: props,
				}).
				Return(nil, nil)
			_, err := c.ExecContext(ctx, query, nv)
			assert.NoError(t, err)
		})
	})
}
