package client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"

	"golang.org/x/crypto/ssh"
)

type SSHClient struct {
	config *ssh.ClientConfig
	port   string
}

func NewSSHClient(user, port, keyfile string) (*SSHClient, error) {
	key, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}

	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil }),
	}

	return &SSHClient{config: config, port: port}, nil
}

func (s *SSHClient) DialAndExecute(address string, commands ...shellCmd) (*bytes.Buffer, error) {
	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", address, s.port), s.config)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var buffer *bytes.Buffer
	for _, cmd := range commands {
		session, err := conn.NewSession()
		if err != nil {
			return nil, err
		}
		buffer, err = s.execute(session, cmd)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func (s *SSHClient) execute(session *ssh.Session, cmd shellCmd) (*bytes.Buffer, error) {
	defer session.Close()

	var out, sessionErr bytes.Buffer
	session.Stdout = &out
	session.Stderr = &sessionErr
	err := session.Run(cmd.Get())
	if err != nil {
		return &sessionErr, err
	}

	return &out, nil
}
