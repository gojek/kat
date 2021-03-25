package client

import "github.com/stretchr/testify/mock"

type MockKafkaAPIClient struct {
	mock.Mock
}

func (m *MockKafkaAPIClient) CreateTopic(topic string, detail TopicDetail, validateOnly bool) error {
	args := m.Called(topic, detail, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaAPIClient) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	args := m.Called(topic, count, assignment, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaAPIClient) DeleteTopic(topics []string) error {
	args := m.Called(topics)
	return args.Error(0)
}

func (m *MockKafkaAPIClient) ListBrokers() map[int]string {
	args := m.Called()
	return args.Get(0).(map[int]string)
}

func (m *MockKafkaAPIClient) ListTopicDetails() (map[string]TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]TopicDetail), args.Error(1)
}

func (m *MockKafkaAPIClient) DescribeTopicMetadata(topics []string) ([]*TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*TopicMetadata), args.Error(1)
}

func (m *MockKafkaAPIClient) UpdateConfig(resourceType int, name string, entries map[string]*string, validateOnly bool) error {
	args := m.Called(resourceType, name, entries, validateOnly)
	return args.Error(0)
}

func (m *MockKafkaAPIClient) GetTopicResourceType() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockKafkaAPIClient) GetConfig(resource ConfigResource) ([]ConfigEntry, error) {
	args := m.Called(resource)
	return args.Get(0).([]ConfigEntry), args.Error(1)
}

func (m *MockKafkaAPIClient) DescribeLogDirs(brokerIDs []int32) (map[int32][]DescribeLogDirsResponseDirMetadata, error) {
	args := m.Called(brokerIDs)
	if args.Get(0) != nil {
		return args.Get(0).(map[int32][]DescribeLogDirsResponseDirMetadata), args.Error(1)
	}
	return nil, args.Error(1)
}
