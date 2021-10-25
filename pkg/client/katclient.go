package client

type Creator interface {
	Create(topic string, detail TopicDetail, validateOnly bool) error
	CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error
}

type Lister interface {
	List() (map[string]TopicDetail, error)
	ListLastWrittenTopics(int64, string) ([]string, error)
	ListOnly(regex string, include bool) ([]string, error)
	ListTopicWithSizeLessThanOrEqualTo(int64) ([]string, error)
}

type Describer interface {
	Describe(topics []string) ([]*TopicMetadata, error)
}

type Configurer interface {
	GetConfig(topic string) ([]ConfigEntry, error)
	UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error
}

type Deleter interface {
	Delete(topics []string) error
}

type Partitioner interface {
	ReassignPartitions(topics []string, brokerList string, topicBatchSize, timeoutPerBatchInS, pollIntervalInS, throttle, partitionBatchSize int) error
	IncreaseReplication(topicsMetadata []*TopicMetadata, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int) error
}
