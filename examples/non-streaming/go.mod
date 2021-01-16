module github.com/vancelongwill/ksql/examples/non-streaming

go 1.15

replace github.com/vancelongwill/ksql => ../../

require (
	github.com/jmoiron/sqlx v1.2.0
	github.com/vancelongwill/ksql v0.0.0-00010101000000-000000000000
	google.golang.org/appengine v1.6.7 // indirect
)
