package pkg

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type SaramaClient struct {
	sarama.ClusterAdmin
}

func NewSaramaClient(admin sarama.ClusterAdmin) *SaramaClient {
	return &SaramaClient{admin}
}

func (s *SaramaClient) ListTopicDetails() (map[string]TopicDetail, error) {
	topics, err := s.ListTopics()
	if err != nil {
		fmt.Printf("Err while retrieving Topic details: %detail\n", err)
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

func (s *SaramaClient) DescribeTopicMetadata(topics []string) ([]*TopicMetadata, error) {
	metadata, err := s.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Err while retrieving Topic metadata: %v\n", err)
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
	err := s.AlterConfig(sarama.ConfigResourceType(resourceType), name, entries, validateOnly)
	if err != nil {
		fmt.Printf("Error while changing config for topic %v - %v\n", name, err)
	}
	return err
}

func (s *SaramaClient) GetTopicResourceType() int {
	return int(sarama.TopicResource)
}

func (s *SaramaClient) ShowConfig(resource ConfigResource) ([]ConfigEntry, error) {
	entries, err := s.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.ConfigResourceType(resource.Type),
		Name:        resource.Name,
		ConfigNames: resource.ConfigNames,
	})
	if err != nil {
		fmt.Printf("Error while retrieving config for %v - %v\n", resource.Name, err)
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
