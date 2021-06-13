
# This makefile defines the following targets
#
#   - all (default) - downloads vendor libs, and build executable
#   - fmt - formats the code
#   - vendor - download all third party libraries and puts them inside vendor directory
#   - clean-vendor - removes third party libraries from vendor directory
#   - hoh-transport-bridge - builds hub-of-hubs-transport-bridge as an executable and puts it under build/bin
#   - clean - cleans the build area (all executables under build/bin)
#   - clean-all - superset of 'clean' that also removes vendor dir

.PHONY: all				##downloads vendor libs, and build executable
all: fmt vendor hoh-transport-bridge

.PHONY: fmt				##formats the code
fmt:
	@go fmt ./...

.PHONY: vendor			##download all third party libraries and puts them inside vendor directory
vendor:
	@go mod vendor

.PHONY: clean-vendor			##removes third party libraries from vendor directory
clean-vendor:
	-@rm -rf vendor

.PHONY: hoh-transport-bridge		##builds hub-of-hubs-transport-bridge as an executable and puts it under build/bin
hoh-transport-bridge:
	@go build -o build/bin/hoh-transport-bridge cmd/main.go

.PHONY: clean			##cleans the build area (all executables under build/bin)
clean:
	@rm -rf build/bin

.PHONY: clean-all			##superset of 'clean' that also removes vendor dir
clean-all: clean-vendor clean

.PHONY: help				##show this help message
help:
	@echo "usage: make [target]\n"; echo "options:"; \fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//' | sed 's/.PHONY:*//' | sed -e 's/^/  /'; echo "";