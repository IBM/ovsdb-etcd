.PHONY: all test test-local test-ci install-deps lint fmt vet

all: test

test-local: install-deps fmt lint vet
	@echo "+ $@"
	@go test -race -v ./...

test:
	@docker-compose run --rm test

# Because CircleCI fails to rm a container
test-ci:
	@docker-compose run test

install-deps:
	@echo "+ $@"
	@go get -u golang.org/x/lint/golint
	@go get -d ./...

lint:
	@echo "+ $@"
	@test -z "$$(golint ./... | tee /dev/stderr)"

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)"

vet:
	@echo "+ $@"
	@go vet ./...

TEST_PATTERN ?= TestSchema

.PHONY: dlv
dlv:
	dlv test --build-flags="-gcflags '-N -l'" -- -test.v -test.run ${TEST_PATTERN}
