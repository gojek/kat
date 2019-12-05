package util

import (
	"bytes"
	"log"
	"os"
	"os/exec"
)

type Executor struct {}

func (e *Executor) Execute(name string, args []string) (bytes.Buffer, error) {
	var out bytes.Buffer
	execCmd := exec.Command(name, args...)
	log.Printf("[Executor] Executing command: %s %v", name, args)
	execCmd.Stdout = &out
	execCmd.Stdin = os.Stdin
	execCmd.Stderr = os.Stderr
	err := execCmd.Run()
	return out, err
}
