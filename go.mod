module github.com/r-che/dfi

go 1.18

require (
	github.com/RediSearch/redisearch-go v1.1.1
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gomodule/redigo v1.8.3
	github.com/r-che/log v0.0.0-00010101000000-000000000000
	github.com/r-che/optsparser v0.0.0-00010101000000-000000000000
	github.com/r-che/testing v0.0.0-00010101000000-000000000000
	go.mongodb.org/mongo-driver v1.10.3
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/montanaflynn/stats v0.0.0-20171201202039-1bf9dbcd8cbe // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.1 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20220908164124-27713097b956 // indirect
	golang.org/x/text v0.3.7 // indirect
)

//replace github.com/r-che/optsparser => ../optsparser
replace github.com/r-che/optsparser => services-code.local/optsparser.git v0.1.10

//replace github.com/r-che/log => ../log
replace github.com/r-che/log => services-code.local/log.git v0.1.12

//replace github.com/r-che/testing => ../testing
replace github.com/r-che/testing => services-code.local/testing.git v0.1.1
