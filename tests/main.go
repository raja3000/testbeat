package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/flowbeat/beater"
)

func main() {
	err := beat.Run("flowbeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
