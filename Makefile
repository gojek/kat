all: build

build:
	go mod download
	go build -o kat
