package pkg

import (
	"bytes"

	"github.com/stretchr/testify/mock"
)

type MockSSHCli struct {
	mock.Mock
}

func (m *MockSSHCli) DialAndExecute(address string, commands ...shellCmd) (*bytes.Buffer, error) {
	args := m.Called(address, commands)
	return args.Get(0).(*bytes.Buffer), args.Error(1)
}
