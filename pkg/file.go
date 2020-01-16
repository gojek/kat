package pkg

import "io/ioutil"

type File struct{}

func (f *File) Write(fileName, data string) error {
	return ioutil.WriteFile(fileName, []byte(data), 0644)
}
