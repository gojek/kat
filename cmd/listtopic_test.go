package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"

	"github.com/gojekfarm/kat/testutil"
)

func TestList(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &testutil.MockTopicCli{}
	TopicCli.(*testutil.MockTopicCli).On("List").Return(map[string]pkg.TopicDetail{}, nil).Times(1)
	l := listTopic{replicationFactor: 1}
	l.listTopic()
	TopicCli.(*testutil.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
