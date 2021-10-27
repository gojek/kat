package io

import (
	"bytes"
	"os"
	"os/exec"
	"syscall"

	"github.com/gojek/kat/logger"
)

type Executor struct{}

func (e *Executor) Execute(name string, args []string) (bytes.Buffer, error) {
	var out bytes.Buffer
	execCmd := exec.Command(name, args...)
	// set the executor command in a different process group,
	// so that it does not inherit the interrupt signal from parent and quit.
	execCmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	logger.Infof("[Executor] Executing command: %s %v", name, args)
	execCmd.Stdout = &out
	execCmd.Stdin = os.Stdin
	execCmd.Stderr = os.Stderr
	err := execCmd.Run()
	return out, err
}
