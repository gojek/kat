package pkg

type TopicDetail struct {
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	Config            map[string]*string
}

type TopicMetadata struct {
	Err        error
	Name       string
	IsInternal bool
	Partitions []*PartitionMetadata
}

type PartitionMetadata struct {
	Err             error
	ID              int32
	Leader          int32
	Replicas        []int32
	Isr             []int32
	OfflineReplicas []int32
}

type ConfigResource struct {
	Type        int
	Name        string
	ConfigNames []string
}

type ConfigEntry struct {
	Name      string
	Value     string
	ReadOnly  bool
	Default   bool
	Source    string
	Sensitive bool
	Synonyms  []*ConfigSynonym
}

type ConfigSynonym struct {
	ConfigName  string
	ConfigValue string
	Source      string
}

type KafkaClient interface {
	ListBrokers() map[int]string
	ListTopicDetails() (map[string]TopicDetail, error)
	DescribeTopicMetadata(topics []string) ([]*TopicMetadata, error)
	UpdateConfig(resourceType int, name string, entries map[string]*string, validateOnly bool) error
	GetTopicResourceType() int
	ShowConfig(resource ConfigResource) ([]ConfigEntry, error)
}
