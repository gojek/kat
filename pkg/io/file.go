package io

import (
	"io/ioutil"
	"os"
)

type File struct{}

func (f *File) Read(fileName string) ([]byte, error) {
	return ioutil.ReadFile(fileName)
}

func (f *File) Write(fileName, data string) error {
	return ioutil.WriteFile(fileName, []byte(data), 0644)
}

func (f *File) Remove(fileName string) error {
	return os.Remove(fileName)
}
