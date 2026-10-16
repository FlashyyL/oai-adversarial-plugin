GO ?= go

.PHONY: build test vet integration clean

build:
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o build/oai-adversarial-plugin.so ./src

test:
	CGO_ENABLED=1 $(GO) test -count=1 ./src

vet:
	CGO_ENABLED=1 $(GO) vet ./src

# Isolated end-to-end test against a real CPA binary and a mock upstream.
# Requires env: CPA_INTEGRATION_BINARY, CPA_INTEGRATION_PLUGIN
integration:
	CGO_ENABLED=1 $(GO) test -count=1 -run TestCPAIntegration -v ./src

clean:
	rm -rf build
