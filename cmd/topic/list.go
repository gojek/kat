package topic

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/topicutil"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

type list struct {
	admin             sarama.ClusterAdmin
	replicationFactor int
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		l := list{admin: u.GetAdminClient("broker-list"), replicationFactor: u.GetIntArg("replication-factor")}
		l.List()
	},
}

func init() {
	listCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
}

func (l *list) List() {
	topicDetails := topicutil.ListTopicDetails(l.admin)
	if topicDetails == nil {
		return
	}
	for topicDetail := range topicDetails {
		if l.replicationFactor != 0 {
			if int(topicDetails[topicDetail].ReplicationFactor) == l.replicationFactor {
				fmt.Println(topicDetail)
			}
		} else {
			fmt.Println(topicDetail)
		}
	}
}
