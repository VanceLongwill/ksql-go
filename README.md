# Kafka ksqlDB driver

https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/contributing/

## Features

- Compatible with the `database/sql` driver
- Supports all the features of the ksqlDB REST API
- Provides high level API for working with pull & pull queries


## Developing 
1. Pull the repo
2. ```shell
go mod download
```
3. ```shell
make coverage
```


## TODO:

- [x] TLS support (use custom http client)
- [x] More examples
- [x] GoDoc
- [x] Passes golint & go vet
- [ ] More tests (see coverage reports)
- [ ] User documentation
- [ ] More semantic error handling
- [ ] Handle HTTP error codes
