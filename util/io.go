package util

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type IO struct{}

func (i *IO) WriteFile(fileName, data string) error {
	return ioutil.WriteFile(fileName, []byte(data), 0644)
}

func (i *IO) AskForConfirmation(question string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", question)

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
