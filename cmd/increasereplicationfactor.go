package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

type increaseReplication struct {
	topics            []string
	replicationFactor int
	numOfBrokers      int
	kafkaPath         string
	zookeeper         string
}

var increaseReplicationFactorCmd = &cobra.Command{
	Use:   "increase-replication-factor",
	Short: "Increases the replication factor for the given topics by the given number",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		i := increaseReplication{topics: Cobra.GetTopicNames(), replicationFactor: Cobra.GetIntArg("replication-factor"), numOfBrokers: Cobra.GetIntArg("num-of-brokers"),
			kafkaPath: Cobra.GetCmdArg("kafka-path"), zookeeper: Cobra.GetCmdArg("zookeeper")}
		i.increaseReplicationFactor()
	},
	PostRun: clearTopicCli,
}

func init() {
	increaseReplicationFactorCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	increaseReplicationFactorCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	increaseReplicationFactorCmd.PersistentFlags().IntP("replication-factor", "r", 0, "New Replication Factor")
	increaseReplicationFactorCmd.PersistentFlags().IntP("num-of-brokers", "n", 0, "Number of brokers in the cluster")
	increaseReplicationFactorCmd.PersistentFlags().StringP("kafka-path", "p", "", "Path to the kafka executable")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("topics")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("zookeeper")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("replication-factor")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("num-of-brokers")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("kafka-path")
}

func (i *increaseReplication) increaseReplicationFactor() {
	err := TopicCli.IncreaseReplicationFactor(i.topics, i.replicationFactor, i.numOfBrokers, i.kafkaPath, i.zookeeper)
	if err != nil {
		fmt.Printf("Error while increasing replication factor: %v\n", err)
		return
	}
}
