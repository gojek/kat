package topic

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildReassignmentJson(suite *testing.T) {
	suite.Run("Build Reassignment Json", func(t *testing.T) {
		partitionMetadata1 := sarama.PartitionMetadata{ID: 8, Leader: 6, Replicas: []int32{6}}
		partitionMetadata2 := sarama.PartitionMetadata{ID: 11, Leader: 3, Replicas: []int32{3}}
		partitionMetadata3 := sarama.PartitionMetadata{ID: 2, Leader: 6, Replicas: []int32{6}}
		partitionMetadata4 := sarama.PartitionMetadata{ID: 5, Leader: 3, Replicas: []int32{3}}
		partitionMetadata5 := sarama.PartitionMetadata{ID: 4, Leader: 2, Replicas: []int32{2}}
		partitionMetadata6 := sarama.PartitionMetadata{ID: 7, Leader: 5, Replicas: []int32{5}}
		partitionMetadata7 := sarama.PartitionMetadata{ID: 10, Leader: 2, Replicas: []int32{2}}
		partitionMetadata8 := sarama.PartitionMetadata{ID: 1, Leader: 5, Replicas: []int32{5}}
		partitionMetadata9 := sarama.PartitionMetadata{ID: 9, Leader: 1, Replicas: []int32{1}}
		partitionMetadata10 := sarama.PartitionMetadata{ID: 3, Leader: 1, Replicas: []int32{1}}
		partitionMetadata11 := sarama.PartitionMetadata{ID: 6, Leader: 4, Replicas: []int32{4}}
		partitionMetadata12 := sarama.PartitionMetadata{ID: 0, Leader: 4, Replicas: []int32{4}}
		topicMetadata := sarama.TopicMetadata{Name: "topic", Partitions: []*sarama.PartitionMetadata{&partitionMetadata1, &partitionMetadata2, &partitionMetadata3, &partitionMetadata4, &partitionMetadata5, &partitionMetadata6, &partitionMetadata7, &partitionMetadata8, &partitionMetadata9, &partitionMetadata10, &partitionMetadata11, &partitionMetadata12}}
		expectedJSONForReplicationFactor3 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 3, 4}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 6, 1}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 5, 6}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 2, 3}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 4, 5}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 1, 2}}}}
		expectedJSONForReplicationFactor4 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2, 3}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5, 6}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 4, 5, 1}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 1, 2, 4}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4, 5}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1, 2}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 6, 1, 3}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 3, 4, 6}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3, 4}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 5, 6, 2}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6, 1}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 2, 3, 5}}}}

		actualJSONForReplicationFactor3 := buildReassignmentJSON(topicMetadata, 3, 6)
		actualJSONForReplicationFactor4 := buildReassignmentJSON(topicMetadata, 4, 6)

		assert.Equal(t, expectedJSONForReplicationFactor3, actualJSONForReplicationFactor3)
		assert.Equal(t, expectedJSONForReplicationFactor4, actualJSONForReplicationFactor4)
	})
}
