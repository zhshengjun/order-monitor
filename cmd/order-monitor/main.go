package main

import (
	"log"

	"order-monitor/internal/app"
)

func main() {
	dropPrivileges()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
