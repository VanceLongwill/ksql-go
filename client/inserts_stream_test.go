package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/ksql/client/internal/testutils"
)

type readCloser struct {
	rdr    io.Reader
	closed bool
	err    error
}

func (r *readCloser) Read(b []byte) (int, error) {
	return r.rdr.Read(b)
}

func (r *readCloser) Close() error {
	r.closed = true
	return r.err
}

func TestInsertsStreamCloser(t *testing.T) {
	t.Run("when the req & resp close without errors", func(t *testing.T) {
		req := &readCloser{rdr: strings.NewReader("")}
		b := strings.NewReader("some body")
		resp := &readCloser{rdr: b}
		cl := &InsertsStreamCloser{
			req:  req,
			resp: resp,
		}
		err := cl.Close()
		assert.NoError(t, err)
		assert.Equal(t, 0, b.Len(), "the response should be emptied")
		assert.True(t, req.closed)
		assert.True(t, resp.closed)
	})
	t.Run("when the req closes with errors", func(t *testing.T) {
		req := &readCloser{rdr: strings.NewReader(""), err: errors.New("some err")}
		b := strings.NewReader("some body")
		resp := &readCloser{rdr: b}
		cl := &InsertsStreamCloser{
			req:  req,
			resp: resp,
		}
		err := cl.Close()
		assert.Error(t, err)
		assert.Equal(t, 9, b.Len(), "the response should NOT be emptied")
	})
}

func TestInsertsStream(t *testing.T) {
	type DataRow struct {
		K  string `json:"k"`
		V1 int    `json:"v1"`
		V2 string `json:"v2"`
		V3 bool   `json:"v3"`
	}
	t.Run("when the server responds with successful acks", func(t *testing.T) {
		payload := InsertsStreamTargetPayload{
			Target: "sometarget",
		}
		writes := []interface{}{
			DataRow{K: "a", V1: 99, V2: "yes", V3: true},
			DataRow{K: "b", V1: 19292, V2: "asdasd", V3: false},
			DataRow{K: "c", V1: 19292, V2: "asdasd", V3: false},
		}
		var acks []interface{}
		for i := range writes {
			acks = append(acks, &InsertsStreamAck{
				Status: "ok",
				Seq:    int64(i),
			})
		}
		srv := testutils.Server(
			insertsStreamPath,
			func(w http.ResponseWriter, r *http.Request) {
				flusher, ok := w.(http.Flusher)
				if !ok {
					panic("expected flusher support")
				}
				dec := json.NewDecoder(r.Body)
				var gotPayload InsertsStreamTargetPayload
				err := dec.Decode(&gotPayload)
				assert.NoError(t, err)
				assert.Equal(t, payload, gotPayload)

				w.WriteHeader(200)
				flusher.Flush()

				enc := json.NewEncoder(w)
				for i, expected := range writes {
					var insert DataRow
					err = dec.Decode(&insert)
					assert.NoError(t, err)
					assert.Equal(t, expected, insert)
					time.Sleep(time.Millisecond * 100)
					err = enc.Encode(&InsertsStreamAck{
						Seq:    int64(i),
						Status: "ok",
					})
					assert.NoError(t, err)
					flusher.Flush()
				}
			},
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.ClientForServer(srv)))
		wtr, err := c.InsertsStream(context.Background(), payload)
		assert.NoError(t, err)
		for _, w := range writes {
			err := wtr.WriteJSON(context.Background(), &w)
			assert.NoError(t, err)
		}
	})
}
