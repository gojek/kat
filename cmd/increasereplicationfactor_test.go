package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestIncreaseReplicationFactor(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	kafkaPath := "/tmp"
	zookeeper := "zookeeper-host"

	TopicCli.(*pkg.MockTopicCli).On("IncreaseReplicationFactor", topics, replicationFactor, numBrokers, kafkaPath, zookeeper).Return(nil).Times(1)
	i := increaseReplication{replicationFactor: replicationFactor, topics: topics, numOfBrokers: numBrokers, kafkaPath: kafkaPath, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
