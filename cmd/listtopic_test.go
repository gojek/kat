package cmd

import (
	"testing"

	"github.com/gojekfarm/kat/pkg"
)

func TestList(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockTopicCli.On("List").Return(map[string]pkg.TopicDetail{}, nil).Times(1)
	l := listTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, replicationFactor: 1}
	l.listTopic()
	mockTopicCli.AssertExpectations(t)
}
