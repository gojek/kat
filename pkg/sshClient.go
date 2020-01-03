package pkg

import (
	"bytes"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"net"
)

type sshClient struct {
	config *ssh.ClientConfig
	port   string
}

func NewSshClient(user, port, keyfile string) (*sshClient, error) {
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

	return &sshClient{config: config, port: port}, nil
}

func (s *sshClient) DialAndExecute(address string, commands ...string) (*bytes.Buffer, error) {
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

func (s *sshClient) dial(address string) (*ssh.Client, error) {
	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", address, s.port), s.config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (s *sshClient) execute(session *ssh.Session, cmd string) (*bytes.Buffer, error) {
	defer session.Close()

	var out, sessionErr bytes.Buffer
	session.Stdout = &out
	session.Stderr = &sessionErr
	err := session.Run(cmd)
	if err != nil {
		return &sessionErr, err
	}

	return &out, nil
}
