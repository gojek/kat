package main

import (
	"github.com/gojekfarm/kat/cmd"
	"github.com/gojekfarm/kat/logger"
)

func main() {
	logger.SetupLogger("info")
	cmd.Execute()
}
