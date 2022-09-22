module github.com/r-che/dfi

go 1.18

require (
	github.com/RediSearch/redisearch-go v1.1.1
	github.com/fsnotify/fsnotify v1.5.4
	github.com/go-redis/redis/v8 v8.11.5
	github.com/r-che/log v0.0.0-00010101000000-000000000000
	github.com/r-che/optsparser v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gomodule/redigo v1.8.3 // indirect
	golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect
)

//replace github.com/r-che/optsparser => ../optsparser
replace github.com/r-che/optsparser => services-code.local/optsparser.git v0.1.3

//replace github.com/r-che/log => ../log
replace github.com/r-che/log => services-code.local/log.git v0.1.4
