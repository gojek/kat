package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestDescribe(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	topics := []string{"topic1"}
	TopicCli.(*pkg.MockTopicCli).On("Describe", topics).Return([]*pkg.TopicMetadata{}, nil).Times(1)
	d := describeTopic{topics: topics}
	d.describeTopic()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
