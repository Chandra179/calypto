package main

import (
	"gosdk/cfg"
	"gosdk/pkg/logger"
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
	// logger
	// ============
	zlogger := logger.NewZeroLog(config.AppEnv)
	testLogger(zlogger)

}

func testLogger(log logger.Logger) {
	log.Info("testing logger",
		logger.Field{Key: "test", Value: "test 123"},
	)
}
