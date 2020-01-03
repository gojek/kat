package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

type increaseReplication struct {
	topics             string
	replicationFactor  int
	numOfBrokers       int
	zookeeper          string
	batch              int
	timeoutPerBatchInS int
	pollIntervalInS    int
	throttle           int
}

var increaseReplicationFactorCmd = &cobra.Command{
	Use:    "increase-replication-factor",
	Short:  "Increases the replication factor for the given topics by the given number",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		i := increaseReplication{topics: Cobra.GetCmdArg("topics"), replicationFactor: Cobra.GetIntArg("replication-factor"), numOfBrokers: Cobra.GetIntArg("num-of-brokers"),
			zookeeper: Cobra.GetCmdArg("zookeeper"), batch: Cobra.GetIntArg("batch"),
			timeoutPerBatchInS: Cobra.GetIntArg("timeout-per-batch"), pollIntervalInS: Cobra.GetIntArg("status-poll-interval"),
			throttle: Cobra.GetIntArg("throttle")}
		i.increaseReplicationFactor()
	},
	PostRun: clearTopicCli,
}

func init() {
	increaseReplicationFactorCmd.PersistentFlags().StringP("topics", "t", "", "Regex to match the topics that need increase in replication factor. eg: \".*\", \"test-.*-topic\", \"topic1|topic2\"")
	increaseReplicationFactorCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	increaseReplicationFactorCmd.PersistentFlags().IntP("replication-factor", "r", 0, "New Replication Factor")
	increaseReplicationFactorCmd.PersistentFlags().IntP("num-of-brokers", "n", 0, "Number of brokers in the cluster")
	increaseReplicationFactorCmd.PersistentFlags().IntP("batch", "", 1, "Batch size to split reassignment")
	increaseReplicationFactorCmd.PersistentFlags().IntP("timeout-per-batch", "", 300, "Timeout for reassignment per batch in seconds")
	increaseReplicationFactorCmd.PersistentFlags().IntP("status-poll-interval", "", 5, "Interval in seconds for polling for reassignment status")
	increaseReplicationFactorCmd.PersistentFlags().IntP("throttle", "", 10000000, "Throttle for reassignment in bytes/sec")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("topics")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("zookeeper")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("replication-factor")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("num-of-brokers")
}

func (i *increaseReplication) increaseReplicationFactor() {
	topics, err := TopicCli.ListOnly(i.topics, true)
	if err != nil {
		fmt.Printf("Error while filtering topics - %v\n", err)
		return
	}

	if len(topics) == 0 {
		fmt.Printf("Did not find any topic matching - %v\n", i.topics)
		return
	}

	err = TopicCli.IncreaseReplicationFactor(topics, i.replicationFactor, i.numOfBrokers, i.batch, i.timeoutPerBatchInS, i.pollIntervalInS, i.throttle, i.zookeeper)
	if err != nil {
		fmt.Printf("Error while increasing replication factor: %v\n", err)
		return
	}
	fmt.Println("Successfully increased replication factor")
}
