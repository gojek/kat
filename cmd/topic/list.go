package topic

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kat/util"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		admin := u.GetAdminClient()
		replicationFactor := u.GetIntArg("replication-factor")
		list(admin, replicationFactor)
	},
}

func init() {
	listCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
}

func list(admin sarama.ClusterAdmin, replicationFactor int) {
	topicDetails, err := admin.ListTopics()
	if err != nil {
		fmt.Printf("Err while retrieving topic details: %v\n", err)
		return
	}
	for topicDetail := range topicDetails {
		if replicationFactor != 0 {
			if int(topicDetails[topicDetail].ReplicationFactor) == replicationFactor {
				fmt.Println(topicDetail)
			}
		} else {
			fmt.Println(topicDetail)
		}
	}
}
