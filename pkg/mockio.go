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
