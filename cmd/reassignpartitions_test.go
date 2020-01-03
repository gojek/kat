package cmd

import (
	"errors"
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestReassignPartitions_Success(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topics := []string{"topic-1"}
	topicRegex := "topic-1"

	TopicCli.(*pkg.MockTopicCli).On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	TopicCli.(*pkg.MockTopicCli).On("ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper).Return(nil).Times(1)
	r := reassignPartitions{topics: topicRegex, brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	r.reassignPartitions()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}

func TestReassignPartitions_ListFailure(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	var topics []string
	topicRegex := "topic-1"

	TopicCli.(*pkg.MockTopicCli).On("ListOnly", topicRegex, true).Return(topics, errors.New("error")).Times(1)
	r := reassignPartitions{topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	r.reassignPartitions()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}

func TestReassignPartitions_NoMatch(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	var topics []string
	topicRegex := "topic-1"

	TopicCli.(*pkg.MockTopicCli).On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	r := reassignPartitions{topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	r.reassignPartitions()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
