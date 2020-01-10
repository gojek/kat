package cmd

import (
	"strings"

	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/gojekfarm/kat/util"
	"github.com/kevinburke/ssh_config"
	"github.com/mitchellh/go-homedir"
)

type BaseCmd struct {
	cobraUtil  *util.CobraUtil
	enableSSH  bool
	addrConfig string
	TopicCli   pkg.TopicCli
}

type Opts func(cmd *BaseCmd)

func Init(cobraUtil *util.CobraUtil, opts ...Opts) BaseCmd {
	baseCmd := &BaseCmd{cobraUtil: cobraUtil, enableSSH: false, addrConfig: "broker-list"}

	for _, opt := range opts {
		opt(baseCmd)
	}

	baseCmd.setTopicCli()
	return *baseCmd
}

func WithSSH() Opts {
	return func(baseCmd *BaseCmd) {
		baseCmd.enableSSH = true
	}
}

func WithAddr(addrConfig string) Opts {
	return func(baseCmd *BaseCmd) {
		baseCmd.addrConfig = addrConfig
	}
}

func (b *BaseCmd) setTopicCli() {
	var err error
	addr := strings.Split(b.cobraUtil.GetStringArg(b.addrConfig), ",")
	var opts []pkg.TopicOpts
	if b.enableSSH {
		keyFile, err := homedir.Expand(b.cobraUtil.GetStringArg("ssh-key-file-path"))
		if err != nil {
			logger.Fatalf("Error while resolving ssh data directory - %v\n", err)
		}
		opts = append(opts, pkg.WithSSHClient(ssh_config.Get("*", "User"), b.cobraUtil.GetStringArg("ssh-port"), keyFile))
	}
	topicCli, err := pkg.NewTopic(pkg.NewSaramaClient(addr), opts...)
	if err != nil {
		logger.Fatalf("Err on creating topic client - %v\n", err)
	}

	b.TopicCli = topicCli
}
