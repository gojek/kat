package pkg

import (
	"bytes"
	"github.com/stretchr/testify/mock"
)

type MockSshCli struct {
	mock.Mock
}

func (m *MockSshCli) DialAndExecute(address string, commands ...shellCmd) (*bytes.Buffer, error) {
	args := m.Called(address, commands)
	return args.Get(0).(*bytes.Buffer), args.Error(1)
}
