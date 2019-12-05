package pkg

import (
	"github.com/stretchr/testify/mock"
)

type MockTopicCli struct {
	mock.Mock
}

func (m *MockTopicCli) List() (map[string]TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]TopicDetail), args.Error(1)
}

func (m *MockTopicCli) Get(regex string) ([]string, error) {
	args := m.Called(regex)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockTopicCli) Describe(topics []string) ([]*TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*TopicMetadata), args.Error(1)
}

func (m *MockTopicCli) ShowConfig(topic string) ([]ConfigEntry, error) {
	args := m.Called(topic)
	return args.Get(0).([]ConfigEntry), args.Error(1)
}

func (m *MockTopicCli) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	args := m.Called(topics, configMap, validateOnly)
	return args.Error(0)
}

func (m *MockTopicCli) IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, zookeeper string) error {
	args := m.Called(topics, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle, zookeeper)
	return args.Error(0)
}

func (m *MockTopicCli) ReassignPartitions(topics []string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, brokerList, zookeeper string) error {
	args := m.Called(topics, batch, timeoutPerBatchInS, pollIntervalInS, throttle, brokerList, zookeeper)
	return args.Error(0)
}
