package io

import (
	"bytes"

	"github.com/stretchr/testify/mock"
)

type MockExecutor struct {
	mock.Mock
}

func (m *MockExecutor) Execute(name string, args []string) (bytes.Buffer, error) {
	arguments := m.Called(name, args)
	return arguments.Get(0).(bytes.Buffer), arguments.Error(1)
}
