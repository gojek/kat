package describe

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/gojek/kat/logger"
	"github.com/stretchr/testify/assert"

	"github.com/gojek/kat/pkg/client"
)

func init() {
	logger.SetDummyLogger()
}

func TestDescribe_Success(t *testing.T) {
	mockDescriber := &client.MockDescriber{}
	topics := []string{"topic1"}
	mockDescriber.On("Describe", topics).Return([]*client.TopicMetadata{}, nil).Times(1)
	d := describeTopic{Describer: mockDescriber, topics: topics}
	d.describeTopic()
	mockDescriber.AssertExpectations(t)
}

func TestDescribe_Failure(t *testing.T) {
	mockDescriber := &client.MockDescriber{}
	topics := []string{"topic1"}
	mockDescriber.On("Describe", topics).Return([]*client.TopicMetadata{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	d := describeTopic{Describer: mockDescriber, topics: topics}
	assert.PanicsWithValue(t, "os.Exit called", d.describeTopic, "os.Exit was not called")
	mockDescriber.AssertExpectations(t)
}
