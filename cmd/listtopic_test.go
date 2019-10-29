package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestList(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	TopicCli.(*pkg.MockTopicCli).On("List").Return(map[string]pkg.TopicDetail{}, nil).Times(1)
	l := listTopic{replicationFactor: 1}
	l.listTopic()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
