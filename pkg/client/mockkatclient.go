package client

import "github.com/stretchr/testify/mock"

type MockCreator struct {
	mock.Mock
}

func (m *MockCreator) Create(topic string, detail TopicDetail, validateOnly bool) error {
	args := m.Called(topic, detail, validateOnly)
	return args.Error(0)
}

func (m *MockCreator) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	args := m.Called(topic, count, assignment, validateOnly)
	return args.Error(0)
}

type MockLister struct {
	mock.Mock
}

func (m *MockLister) List() (map[string]TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]TopicDetail), args.Error(1)
}

func (m *MockLister) ListLastWrittenTopics(lastWrittenEpoch int64, dataDir string) ([]string, error) {
	args := m.Called(lastWrittenEpoch, dataDir)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockLister) ListOnly(regex string, include bool) ([]string, error) {
	args := m.Called(regex, include)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockLister) ListEmptyLastWrittenTopics(lastWrittenEpoch int64, dataDir string) ([]string, error) {
	args := m.Called(lastWrittenEpoch, dataDir)
	return args.Get(0).([]string), args.Error(1)
}

type MockDescriber struct {
	mock.Mock
}

func (m *MockDescriber) Describe(topics []string) ([]*TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*TopicMetadata), args.Error(1)
}

type MockConfigurer struct {
	mock.Mock
}

func (m *MockConfigurer) GetConfig(topic string) ([]ConfigEntry, error) {
	args := m.Called(topic)
	return args.Get(0).([]ConfigEntry), args.Error(1)
}

func (m *MockConfigurer) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	args := m.Called(topics, configMap, validateOnly)
	return args.Error(0)
}

type MockDeleter struct {
	mock.Mock
}

func (m *MockDeleter) Delete(topics []string) error {
	args := m.Called(topics)
	return args.Error(0)
}

type MockPartitioner struct {
	mock.Mock
}

func (m *MockPartitioner) IncreaseReplication(topicsMetadata []*TopicMetadata, replicationFactor, numOfBrokers, batch,
	timeoutPerBatchInS, pollIntervalInS, throttle int) error {
	args := m.Called(topicsMetadata, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
	return args.Error(0)
}

func (m *MockPartitioner) ReassignPartitions(topics []string, brokerList string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int) error {
	args := m.Called(topics, brokerList, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
	return args.Error(0)
}
