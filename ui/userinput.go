package ui

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/gojekfarm/kat/logger"
)

type UserInput struct{}

func (u UserInput) AskForConfirmation(question string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		logger.Printf("%s [y/n]: ", question)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
	}
}
