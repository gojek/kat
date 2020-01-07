package pkg

import "github.com/stretchr/testify/mock"

type MockKafkaApiClient struct {
	mock.Mock
}

func (m *MockKafkaApiClient) DeleteTopic(topics []string) error {
	args := m.Called(topics)
	return args.Error(0)
}

func (m *MockKafkaApiClient) ListBrokers() map[int]string {
	args := m.Called()
	return args.Get(0).(map[int]string)
}

func (m *MockKafkaApiClient) ListTopicDetails() (map[string]TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]TopicDetail), args.Error(1)
}

func (m *MockKafkaApiClient) DescribeTopicMetadata(topics []string) ([]*TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*TopicMetadata), args.Error(1)
}

func (m *MockKafkaApiClient) UpdateConfig(resourceType int, name string, entries map[string]*string, validateOnly bool) error {
	args := m.Called(resourceType, name, entries, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaApiClient) GetTopicResourceType() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockKafkaApiClient) ShowConfig(resource ConfigResource) ([]ConfigEntry, error) {
	args := m.Called(resource)
	return args.Get(0).([]ConfigEntry), args.Error(1)
}
