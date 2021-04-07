package client

import "github.com/stretchr/testify/mock"

type MockSSHClient struct {
	mock.Mock
}

func (m *MockSSHClient) ListTopics(req ListTopicsRequest) ([]string, error) {
	args := m.Called(req)
	if args.Get(0) != nil {
		return args.Get(0).([]string), args.Error(1)
	}
	return nil, args.Error(1)
}
