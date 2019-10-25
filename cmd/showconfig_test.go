package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"

	"github.com/gojekfarm/kat/testutil"
)

func TestShow(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &testutil.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	TopicCli.(*testutil.MockTopicCli).On("ShowConfig", "topic1").Return([]pkg.ConfigEntry{}, nil).Times(1)
	TopicCli.(*testutil.MockTopicCli).On("ShowConfig", "topic2").Return([]pkg.ConfigEntry{}, nil).Times(1)
	s := showConfig{topics: topics}
	s.showConfig()
	TopicCli.(*testutil.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
