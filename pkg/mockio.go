package pkg

import (
	"github.com/stretchr/testify/mock"
)

type MockIo struct {
	mock.Mock
}

func (m *MockIo) WriteFile(fileName, data string) error {
	arguments := m.Called(fileName, data)
	return arguments.Error(0)
}

func (m *MockIo) AskForConfirmation(question string) bool {
	arguments := m.Called(question)
	return arguments.Bool(0)
}
