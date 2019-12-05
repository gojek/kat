package cmd

import (
	"errors"
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestIncreaseReplicationFactor_Success(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	TopicCli.(*pkg.MockTopicCli).On("Get", topicRegex).Return(topics, nil).Times(1)
	TopicCli.(*pkg.MockTopicCli).On("IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper).Return(nil).Times(1)
	i := increaseReplication{replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}

func TestIncreaseReplicationFactor_GetFailure(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	TopicCli.(*pkg.MockTopicCli).On("Get", topicRegex).Return(topics, errors.New("error")).Times(1)
	i := increaseReplication{replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}

func TestIncreaseReplicationFactor_NoMatch(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	TopicCli.(*pkg.MockTopicCli).On("Get", topicRegex).Return(topics, nil).Times(1)
	i := increaseReplication{replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
