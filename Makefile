all: clean check-quality golangci test build
APP=kat
ALL_PACKAGES=$(shell go list ./...)
SOURCE_DIRS=$(shell go list ./... | cut -d "/" -f4 | uniq)

clean:
	rm -f ./kat
	GO111MODULE=on go mod tidy -v

setup:
	go get -u golang.org/x/tools/cmd/goimports
	go get -u golang.org/x/lint/golint
	go get -u github.com/fzipp/gocyclo

test:
	go test ./...

testcodecov:
	go test -coverprofile=coverage.txt -covermode=atomic ./...

build:
	@echo "Building './kat'..."
	go mod download
	go build -o kat

check-quality: lint fmt imports vet

lint:
	@if [[ `golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; } | wc -l | tr -d ' '` -ne 0 ]]; then \
          golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; }; \
          exit 2; \
    fi;

fmt:
	gofmt -l -s -w $(SOURCE_DIRS)

imports:
	./scripts/lint.sh check_imports

fix_imports:
	goimports -l -w .

vet:
	go vet ./...

golangci:
	GO111MODULE=off go get -v github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run -v --deadline 5m0s

test-coverage:
	mkdir -p ./out
	@echo "mode: count" > coverage-all.out
	$(foreach pkg, $(ALL_PACKAGES),\
	go test -coverprofile=coverage.out -covermode=count $(pkg);\
	tail -n +2 coverage.out >> coverage-all.out;)
	GO111MODULE=on go tool cover -html=coverage-all.out -o ./out/coverage.html
	@echo "---"
	cat ./out/coverage.html | grep "<option" | cut -d ">" -f2 | cut -d "<" -f1 | grep -v "mock" | grep -v "config" | grep -v "stub"