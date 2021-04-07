package list

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockConsumerListener struct{ mock.Mock }

func (m *mockConsumerListener) ListConsumerGroups() (map[string]string, error) {
	args := m.Called()
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *mockConsumerListener) GetConsumerGroupsForTopic(consumerGroups []string, topic string) (chan string, error) {
	args := m.Called(consumerGroups, topic)
	return args.Get(0).(chan string), args.Error(1)
}

func TestListGroupsReturnsSuccess(t *testing.T) {
	mockConsumer := new(mockConsumerListener)
	admin := consumerGroupAdmin{mockConsumer}
	mockChannel := make(chan string, 0)

	consumerGroupsMap := map[string]string{"consumer1": "", "consumer2": ""}
	mockConsumer.On("ListConsumerGroups").Return(consumerGroupsMap, nil)
	validateParams := func(token []string) bool {
		return (token[0] == "consumer1" && token[1] == "consumer2") || (token[0] == "consumer2" && token[1] == "consumer1")
	}
	mockConsumer.On("GetConsumerGroupsForTopic", mock.MatchedBy(validateParams), "").Return(mockChannel, nil)

	err := admin.ListGroups("")

	require.NoError(t, err)
	mockConsumer.AssertExpectations(t)
}

func TestListGroupsReturnsFailureIfListConsumerGroupsFails(t *testing.T) {
	mockConsumer := new(mockConsumerListener)
	admin := consumerGroupAdmin{mockConsumer}
	multipleConsumers := map[string]string{"consumer1": "", "consumer2": ""}
	mockConsumer.On("ListConsumerGroups").Return(multipleConsumers, errors.New("list consumer groups failed"))

	err := admin.ListGroups("")

	require.Error(t, err)
	assert.Equal(t, "list consumer groups failed", err.Error())
	mockConsumer.AssertExpectations(t)
}

func TestListGroupsReturnsFailureIfGetConsumerGroupsFails(t *testing.T) {
	mockConsumer := new(mockConsumerListener)
	admin := consumerGroupAdmin{mockConsumer}
	mockChannel := make(chan string, 0)

	consumerGroupsMap := map[string]string{"consumer1": "", "consumer2": ""}
	mockConsumer.On("ListConsumerGroups").Return(consumerGroupsMap, nil)

	mockConsumer.On("GetConsumerGroupsForTopic", []string{"consumer1", "consumer2"}, "").Return(mockChannel, errors.New("get consumer groups failed"))

	err := admin.ListGroups("")
	require.Error(t, err)
	assert.Equal(t, "get consumer groups failed", err.Error())
	mockConsumer.AssertExpectations(t)
}
