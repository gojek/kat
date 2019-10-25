package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"

	"github.com/gojekfarm/kat/testutil"
)

func TestDescribe(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &testutil.MockTopicCli{}
	topics := []string{"topic1"}
	TopicCli.(*testutil.MockTopicCli).On("Describe", topics).Return([]*pkg.TopicMetadata{}, nil).Times(1)
	d := describeTopic{topics: topics}
	d.describeTopic()
	TopicCli.(*testutil.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
