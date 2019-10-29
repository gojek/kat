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

func (m *MockTopicCli) IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) error {
	args := m.Called(topics, replicationFactor, numOfBrokers, kafkaPath, zookeeper)
	return args.Error(0)
}
