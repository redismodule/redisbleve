# redisbleve

Fulltext search module build on top of [bleve](https://github.com/blevesearch/bleve)

* `ft.create index-name`
    * [ ] `ft.create index-name JSON Mapping`
* `ft.index index-name doc-id doc-content`
* `ft.query index-name query`
    * Query document by [query string](http://www.blevesearch.com/docs/Query-String-Query/)
    * Only return doc-it by default
    * [ ] `ft.query index-name query LIMIT from size`
* `ft.count`
* `ft.del index-name doc-id`

## Install
```bash
# Build module from source
go build -v -buildmode=c-shared github.com/redismodule/redisbleve/cmd/redisbleve
# Load module
redis-server --loadmodule ./redisbleve --loglevel debug
# You can use these commands now.
```

## Test
```basg
ft.create idx
ft.index idx a "redisbleve - Fulltext search module build on top of bleve"
ft.index idx b "bleve - A modern text indexing library for go"
ft.query idx bleve
ft.query idx redis
```
