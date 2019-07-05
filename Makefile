all: clean check-quality test build
ALL_PACKAGES=$(shell go list ./...)
SOURCE_DIRS=$(shell go list ./... | cut -d "/" -f4 | uniq)

clean:
	rm -f ./kat
	GO111MODULE=on go mod tidy -v

test:
	go test ./...

build:
	go mod download
	go build -o kat

check-quality: lint fmt vet

lint:
	@if [[ `golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; } | wc -l | tr -d ' '` -ne 0 ]]; then \
          golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; }; \
          exit 2; \
    fi;

fmt:
	gofmt -l -s -w $(SOURCE_DIRS)

vet:
	go vet ./...