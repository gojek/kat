package client

type ConsumerLister interface {
	ListConsumerGroups() (map[string]string, error)
	GetConsumerGroupsForTopic([]string, string) (chan string, error)
}
