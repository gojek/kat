package pkg

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
)

type MockSaramaClient struct{
	mock.Mock
}

func (m *MockSaramaClient) Config() *sarama.Config {
	panic("implement me")
}

func (m *MockSaramaClient) Controller() (*sarama.Broker, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Brokers() []*sarama.Broker {
	args := m.Called()
	return args.Get(0).([]*sarama.Broker)
}

func (m *MockSaramaClient) Topics() ([]string, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Partitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m *MockSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m *MockSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m *MockSaramaClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m *MockSaramaClient) RefreshMetadata(topics ...string) error {
	panic("implement me")
}

func (m *MockSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	panic("implement me")
}

func (m *MockSaramaClient) RefreshCoordinator(consumerGroup string) error {
	panic("implement me")
}

func (m *MockSaramaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	panic("implement me")
}

func (m *MockSaramaClient) Close() error {
	panic("implement me")
}

func (m *MockSaramaClient) Closed() bool {
	panic("implement me")
}
