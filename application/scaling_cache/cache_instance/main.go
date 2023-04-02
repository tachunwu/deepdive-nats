package main

import (
	"fmt"
	"runtime"

	"github.com/dgraph-io/ristretto"
	natsclient "github.com/nats-io/nats.go"
	"github.com/spf13/cast"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

var subject *string = flag.String("subject", "", "Subject that this cache listens to.")
var routers *[]string = flag.StringArray("routers", []string{}, "Router that this cache connects to.")

func main() {

	// Parse flags
	flag.Parse()

	// Set logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Init cache instance
	InitCacheInstance(*routers, *subject, logger)
	runtime.Goexit()
}

type CacheInstance struct {
	cahce *ristretto.Cache
	ncMap map[string]*natsclient.Conn
}

func InitCacheInstance(routers []string, subject string, logger *zap.Logger) {
	cacheInstance := &CacheInstance{
		ncMap: make(map[string]*natsclient.Conn),
	}

	// Init cache instance
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
	cacheInstance.cahce = cache

	// Connect to NATS ambassador router
	for _, router := range routers {
		nc, err := natsclient.Connect(router)
		if err != nil {
			fmt.Println(err)
			return
		}
		nc.Subscribe(subject, func(msg *natsclient.Msg) {
			switch msg.Header.Get("operation") {
			case "set":
				cache.SetWithTTL(
					msg.Header.Get("key"),
					msg.Data,
					1,
					cast.ToDuration(msg.Header.Get("ttl")),
				)
				cache.Wait()

				logger.Info("Set",
					zap.String("key", msg.Header.Get("key")),
					zap.String("value", string(msg.Data)),
					zap.String("ttl", msg.Header.Get("ttl")),
				)
			case "get":
				value, found := cache.Get(msg.Header.Get("key"))
				if !found {
					msg.Respond([]byte("missing value"))
					return
				}
				logger.Info("Get",
					zap.String("key", msg.Header.Get("key")),
					zap.ByteString("value", value.([]byte)),
					zap.String("ttl", msg.Header.Get("ttl")),
				)
				msg.Respond(value.([]byte))

			case "delete":
				cache.Del(msg.Header.Get("key"))
				logger.Info("Delete",
					zap.String("key", msg.Header.Get("key")),
				)
				msg.Respond([]byte("deleted" + msg.Header.Get("key")))
			}
		})
		cacheInstance.ncMap[router] = nc
	}
}
