package cmd

import (
	"testing"

	"github.com/gojekfarm/kat/testutil"
)

func TestAlter(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &testutil.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	config := "key1=val1"
	value := "val1"
	configMap := map[string]*string{"key1": &value}
	TopicCli.(*testutil.MockTopicCli).On("UpdateConfig", topics, configMap, false).Return(nil).Times(1)
	a := alterConfig{topics: topics, config: config}
	a.alterConfig()
	TopicCli.(*testutil.MockTopicCli).AssertExpectations(t)
			clearTopicCli(nil, nil)
}
