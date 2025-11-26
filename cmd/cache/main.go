package main

import (
	"gosdk/cfg"
	"gosdk/pkg/cache"
	"log"
)

func main() {
	// ============
	// config
	// ============
	config, errCfg := cfg.Load()
	if errCfg != nil {
		log.Fatal(errCfg)
	}

	// ============
	// Cache
	// ============
	redisAddr := config.Redis.Host + ":" + config.Redis.Port
	_ = cache.NewRedisCache(redisAddr)
}
