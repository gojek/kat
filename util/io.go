package util

import (
	"io/ioutil"
)

type IO struct{}

func (i *IO) WriteFile(fileName, data string) error {
	return ioutil.WriteFile(fileName, []byte(data), 0644)
}
