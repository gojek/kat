package main

import (
	"github.com/gojek/kat/cmd"
	"github.com/gojek/kat/logger"
)

func main() {
	logger.SetupLogger("info")
	cmd.Execute()
}
