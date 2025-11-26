package main

import (
	"log"

	"gosdk/pkg/passkey"

	"github.com/gin-gonic/gin"
)

func main() {
	storage := passkey.NewInMemoryStorage()

	config := passkey.Config{
		RPDisplayName: "My App",
		RPID:          "c16eff6aa4da.ngrok-free.app",
		RPOrigins:     []string{"https://c16eff6aa4da.ngrok-free.app"},
	}

	handler, err := passkey.NewPasskeyHandler(config, storage)
	if err != nil {
		log.Fatal(err)
	}

	r := gin.Default()

	r.GET("/", handler.ServeHTML)
	handler.RegisterRoutes(r)

	r.Run(":8080")
}
