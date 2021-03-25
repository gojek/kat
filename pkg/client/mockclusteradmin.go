package client

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
)

type MockClusterAdmin struct {
	mock.Mock
}

func (m *MockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	args := m.Called(topic, detail, validateOnly)
	return args.Error(0)
}

func (m *MockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	args := m.Called(topics)
	return args.Get(0).([]*sarama.TopicMetadata), args.Error(1)
}

func (m *MockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

func (m *MockClusterAdmin) DeleteTopic(topic string) error {
	args := m.Called(topic)
	return args.Error(0)
}

func (m *MockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	args := m.Called(topic, count, assignment, validateOnly)
	return args.Error(0)
}

func (m *MockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	args := m.Called(topic, partitionOffsets)
	return args.Error(0)
}

func (m *MockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	args := m.Called(resource)
	return args.Get(0).([]sarama.ConfigEntry), args.Error(1)
}

func (m *MockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	args := m.Called(resourceType, name, entries, validateOnly)
	return args.Error(0)
}

func (m *MockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	args := m.Called(resource, acl)
	return args.Error(0)
}

func (m *MockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	args := m.Called(filter)
	return args.Get(0).([]sarama.ResourceAcls), args.Error(1)
}

func (m *MockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	args := m.Called(filter, validateOnly)
	return args.Get(0).([]sarama.MatchingAcl), args.Error(1)
}

func (m *MockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	args := m.Called()
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	args := m.Called(groups)
	return args.Get(0).([]*sarama.GroupDescription), args.Error(1)
}

func (m *MockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	args := m.Called(group, topicPartitions)
	return args.Get(0).(*sarama.OffsetFetchResponse), args.Error(1)
}

func (m *MockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	args := m.Called()
	return args.Get(0).([]*sarama.Broker), args.Get(1).(int32), args.Error(2)
}

func (m *MockClusterAdmin) DeleteConsumerGroup(group string) error {
	args := m.Called(group)
	return args.Error(0)
}

func (m *MockClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	panic("unused")
}

func (m *MockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	args := m.Called(brokers)
	if args.Get(0) != nil {
		return args.Get(0).(map[int32][]sarama.DescribeLogDirsResponseDirMetadata), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockClusterAdmin) ListPartitionReassignments(topics string,
	partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	panic("unused")
}
