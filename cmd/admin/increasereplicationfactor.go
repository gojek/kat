package admin

import (
	"github.com/gojekfarm/kat/cmd/base"
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg/client"
	"github.com/spf13/cobra"
)

type increaseReplication struct {
	client.Lister
	client.Describer
	client.Partitioner
	topics             string
	replicationFactor  int
	numOfBrokers       int
	batch              int
	timeoutPerBatchInS int
	pollIntervalInS    int
	throttle           int
}

var IncreaseReplicationFactorCmd = &cobra.Command{
	Use:   "increase-replication-factor",
	Short: "Increases the replication factor for the given topics by the given number",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		zookeeper := cobraUtil.GetStringArg("zookeeper")
		baseCmd := base.Init(cobraUtil, base.WithPartition(zookeeper))
		i := increaseReplication{Lister: baseCmd.GetTopic(), Describer: baseCmd.GetTopic(), Partitioner: baseCmd.GetPartition(),
			topics: cobraUtil.GetStringArg("topics"), replicationFactor: cobraUtil.GetIntArg("replication-factor"),
			numOfBrokers: cobraUtil.GetIntArg("num-of-brokers"), batch: cobraUtil.GetIntArg("batch"),
			timeoutPerBatchInS: cobraUtil.GetIntArg("timeout-per-batch"),
			pollIntervalInS:    cobraUtil.GetIntArg("status-poll-interval"), throttle: cobraUtil.GetIntArg("throttle")}
		i.increaseReplicationFactor()
	},
}

func init() {
	IncreaseReplicationFactorCmd.PersistentFlags().StringP("topics", "t", "",
		"Regex to match the topics that need increase in replication factor. eg: \".*\", \"test-.*-topic\", \"topic1|topic2\"")
	IncreaseReplicationFactorCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("replication-factor", "r", 0, "New Replication Factor")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("num-of-brokers", "n", 0, "Number of brokers in the cluster")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("batch", "", 1, "Batch size to split reassignment")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("timeout-per-batch", "", 300, "Timeout for reassignment per batch in seconds")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("status-poll-interval", "", 5, "Interval in seconds for polling for reassignment status")
	IncreaseReplicationFactorCmd.PersistentFlags().IntP("throttle", "", 10000000, "Throttle for reassignment in bytes/sec")
	if err := IncreaseReplicationFactorCmd.MarkPersistentFlagRequired("topics"); err != nil {
		logger.Fatal(err)
	}
	if err := IncreaseReplicationFactorCmd.MarkPersistentFlagRequired("zookeeper"); err != nil {
		logger.Fatal(err)
	}
	if err := IncreaseReplicationFactorCmd.MarkPersistentFlagRequired("replication-factor"); err != nil {
		logger.Fatal(err)
	}
	if err := IncreaseReplicationFactorCmd.MarkPersistentFlagRequired("num-of-brokers"); err != nil {
		logger.Fatal(err)
	}
}

func (i *increaseReplication) increaseReplicationFactor() {
	topics, err := i.ListOnly(i.topics, true)
	if err != nil {
		logger.Fatalf("Error while filtering topics - %v\n", err)
	}

	if len(topics) == 0 {
		logger.Infof("Did not find any topic matching - %v\n", i.topics)
		return
	}

	topicMetadata, err := i.Describe(topics)
	if err != nil {
		logger.Fatalf("Error while fetching topic metadata - %v\n", err)
	}

	err = i.IncreaseReplication(topicMetadata, i.replicationFactor, i.numOfBrokers, i.batch,
		i.timeoutPerBatchInS, i.pollIntervalInS, i.throttle)
	if err != nil {
		logger.Fatalf("Error while increasing replication factor: %v\n", err)
		return
	}
	logger.Info("Successfully increased replication factor")
}
