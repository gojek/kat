package base

import (
	"strings"

	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/kevinburke/ssh_config"
	"github.com/mitchellh/go-homedir"
)

type Cmd struct {
	cobraUtil  *CobraUtil
	enableSSH  bool
	addrConfig string
	TopicCli   pkg.TopicCli
}

type Opts func(cmd *Cmd)

func Init(cobraUtil *CobraUtil, opts ...Opts) Cmd {
	baseCmd := &Cmd{cobraUtil: cobraUtil, enableSSH: false, addrConfig: "broker-list"}

	for _, opt := range opts {
		opt(baseCmd)
	}

	baseCmd.setTopicCli()
	return *baseCmd
}

func WithSSH() Opts {
	return func(baseCmd *Cmd) {
		baseCmd.enableSSH = true
	}
}

func WithAddr(addrConfig string) Opts {
	return func(baseCmd *Cmd) {
		baseCmd.addrConfig = addrConfig
	}
}

func (b *Cmd) setTopicCli() {
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