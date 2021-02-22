
.PHONY: build
build:
	go build

.PHONY: generate
generate:
	go run cmd/codegenerator/generator.go -s ../json/ovn-nb.ovsschema  -d ./json
	go run cmd/codegenerator/generator.go -s ../json/ovn-sb.ovsschema  -d ./json
	go run cmd/codegenerator/generator.go -s ../json/_server.ovsschema  -d ./json
	gofmt -s -w  ./json

