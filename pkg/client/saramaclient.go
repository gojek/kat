package client

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gojek/kat/logger"
)

type SaramaClient struct {
	admin  sarama.ClusterAdmin
	client sarama.Client
}

type consumerGroups map[string]*sarama.GroupMemberDescription

func (c *consumerGroups) HasSubscription(topic string) bool {
	for _, memberDesc := range *c {
		ma, _ := memberDesc.GetMemberAssignment()
		for topicName := range ma.Topics {
			if topicName == topic {
				return true
			}
		}
		break
	}
	return false
}

func NewSaramaClient(addr []string) *SaramaClient {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0

	admin, err := sarama.NewClusterAdmin(addr, cfg)
	if err != nil {
		logger.Fatalf("Err on creating admin for %s: %v\n", addr, err)
	}

	client, err := sarama.NewClient(addr, cfg)
	if err != nil {
		logger.Fatalf("Err on creating client for %s: %v\n", addr, err)
	}
	return &SaramaClient{admin, client}
}

func (s *SaramaClient) CreateTopic(topic string, detail TopicDetail, validateOnly bool) error {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
		ReplicaAssignment: detail.ReplicaAssignment,
		ConfigEntries:     detail.Config,
	}
	return s.admin.CreateTopic(topic, topicDetail, validateOnly)
}

func (s *SaramaClient) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return s.admin.CreatePartitions(topic, count, assignment, validateOnly)
}

func (s *SaramaClient) ListBrokers() map[int]string {
	brokerMap := make(map[int]string)
	brokers := s.client.Brokers()
	for _, broker := range brokers {
		brokerMap[int(broker.ID())] = broker.Addr()
	}
	return brokerMap
}

func (s *SaramaClient) ListConsumerGroups() (map[string]string, error) {
	return s.admin.ListConsumerGroups()
}

func (s *SaramaClient) GetConsumerGroupsForTopic(groups []string, topic string) (chan string, error) {
	var wg sync.WaitGroup
	consumerGroupsChannel := make(chan string, len(groups))

	for i := 0; i < len(groups); i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()
			groupDescription, err := s.admin.DescribeConsumerGroups([]string{groups[i]})
			if err != nil {
				logger.Fatalf("Err on describing consumer group %s: %v\n", groups[i], err)
			}

			var c consumerGroups = groupDescription[0].Members

			if c.HasSubscription(topic) {
				consumerGroupsChannel <- groupDescription[0].GroupId
				fmt.Println(groupDescription[0].GroupId)
			}
		}(i, &wg)
	}

	wg.Wait()

	close(consumerGroupsChannel)

	return consumerGroupsChannel, nil
}

func (s *SaramaClient) ListTopicDetails() (map[string]TopicDetail, error) {
	topics, err := s.admin.ListTopics()
	if err != nil {
		logger.Errorf("Err while retrieving topic details: %detail\n", err)
		return nil, err
	}

	topicDetails := map[string]TopicDetail{}
	for topic, detail := range topics {
		topicDetails[topic] = TopicDetail{
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			ReplicaAssignment: detail.ReplicaAssignment,
			Config:            detail.ConfigEntries,
		}
	}

	return topicDetails, err
}

func (s *SaramaClient) DeleteTopic(topics []string) error {
	for _, topic := range topics {
		err := s.admin.DeleteTopic(topic)
		if err != nil {
			logger.Errorf("Error while deleting topic %v- %v\n", topic, err)
		} else {
			logger.Infof("Deleted topic - %v\n", topic)
		}
	}
	return nil
}

func (s *SaramaClient) DescribeTopicMetadata(topics []string) ([]*TopicMetadata, error) {
	metadata, err := s.admin.DescribeTopics(topics)
	if err != nil {
		logger.Errorf("Err while retrieving topic metadata: %v\n", err)
		return nil, err
	}

	var topicMetadata []*TopicMetadata
	for _, data := range metadata {
		var partitionMetadata []*PartitionMetadata
		for _, partition := range data.Partitions {
			partitionMetadata = append(partitionMetadata, &PartitionMetadata{
				Err:             partition.Err,
				ID:              partition.ID,
				Leader:          partition.Leader,
				Replicas:        partition.Replicas,
				Isr:             partition.Isr,
				OfflineReplicas: partition.OfflineReplicas,
			})
		}
		topicMetadata = append(topicMetadata, &TopicMetadata{
			Err:        data.Err,
			Name:       data.Name,
			IsInternal: data.IsInternal,
			Partitions: partitionMetadata,
		})
	}

	return topicMetadata, nil
}

func (s *SaramaClient) UpdateConfig(resourceType int, name string, entries map[string]*string, validateOnly bool) error {
	err := s.admin.AlterConfig(sarama.ConfigResourceType(resourceType), name, entries, validateOnly)
	if err != nil {
		logger.Errorf("Error while changing config for topic %v - %v\n", name, err)
	}
	return err
}

func (s *SaramaClient) GetTopicResourceType() int {
	return int(sarama.TopicResource)
}

func (s *SaramaClient) GetConfig(resource ConfigResource) ([]ConfigEntry, error) {
	entries, err := s.admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.ConfigResourceType(resource.Type),
		Name:        resource.Name,
		ConfigNames: resource.ConfigNames,
	})
	if err != nil {
		logger.Errorf("Error while retrieving config for %v - %v\n", resource.Name, err)
		return nil, err
	}

	var configEntries []ConfigEntry
	for _, e := range entries {
		var configSynonyms []*ConfigSynonym
		for _, s := range e.Synonyms {
			configSynonyms = append(configSynonyms, &ConfigSynonym{
				ConfigName:  s.ConfigName,
				ConfigValue: s.ConfigValue,
				Source:      s.Source.String(),
			})
		}

		configEntries = append(configEntries, ConfigEntry{
			Name:      e.Name,
			Value:     e.Value,
			ReadOnly:  e.ReadOnly,
			Default:   e.Default,
			Source:    e.Source.String(),
			Sensitive: e.Sensitive,
			Synonyms:  configSynonyms,
		})
	}

	return configEntries, nil
}

func (s *SaramaClient) DescribeLogDirs(brokerIDs []int32) (map[int32][]DescribeLogDirsResponseDirMetadata, error) {
	metaData, err := s.admin.DescribeLogDirs(brokerIDs)
	if err != nil {
		return nil, err
	}
	brokerWiseLogDirsResponseMetaData := make(map[int32][]DescribeLogDirsResponseDirMetadata, len(brokerIDs))
	for brokerID, brokerMetaDataList := range metaData {
		list := make([]DescribeLogDirsResponseDirMetadata, 0)
		for _, logDirsResponseMetaData := range brokerMetaDataList {
			var err error
			if logDirsResponseMetaData.ErrorCode != sarama.ErrNoError {
				err = fmt.Errorf("broker Id: %d, error: %w", brokerID, errors.New(logDirsResponseMetaData.ErrorCode.Error()))
			}
			rMeta := DescribeLogDirsResponseDirMetadata{
				Error:  err,
				Path:   logDirsResponseMetaData.Path,
				Topics: getLogDirsTopics(logDirsResponseMetaData.Topics),
			}
			list = append(list, rMeta)
		}
		brokerWiseLogDirsResponseMetaData[brokerID] = list
	}
	return brokerWiseLogDirsResponseMetaData, nil
}

func getLogDirsTopics(topics []sarama.DescribeLogDirsResponseTopic) []DescribeLogDirsResponseTopic {
	list := make([]DescribeLogDirsResponseTopic, 0, len(topics))
	for _, topic := range topics {
		rTopic := DescribeLogDirsResponseTopic{
			Topic:      topic.Topic,
			Partitions: getLogDirsPartition(topic.Partitions),
		}
		list = append(list, rTopic)
	}
	return list
}

func getLogDirsPartition(partitions []sarama.DescribeLogDirsResponsePartition) []DescribeLogDirsResponsePartition {
	list := make([]DescribeLogDirsResponsePartition, 0, len(partitions))
	for _, partition := range partitions {
		rPartition := DescribeLogDirsResponsePartition{
			PartitionID: partition.PartitionID,
			Size:        partition.Size,
			OffsetLag:   partition.OffsetLag,
			IsTemporary: partition.IsTemporary,
		}
		list = append(list, rPartition)
	}
	return list
}
