BUILD := build
GO ?= go
GOFILES := $(shell find . -name "*.go" -type f)
GOFMT ?= gofmt
GOIMPORTS ?= goimports -local=github.com/jacksontj/promxy
STATICCHECK ?= staticcheck
GOLANGCI_LINT ?= golangci-lint
# Revision new code is compared against in `make lint-new`.
LINT_BASE_REV ?= origin/master

.PHONY: clean
clean:
	$(GO) clean -i ./...
	rm -rf $(BUILD)

.PHONY: static-check
static-check:
	$(STATICCHECK) ./...

# Full golangci-lint pass over the whole module (vendor excluded via config).
.PHONY: lint
lint:
	$(GOLANGCI_LINT) run

# Auto-fix the mechanical findings (gofmt, goimports, whitespace, unconvert...).
.PHONY: lint-fix
lint-fix:
	$(GOLANGCI_LINT) run --fix

# Report only issues on code changed versus LINT_BASE_REV — the gate to run
# after making changes, so the inherited baseline does not drown new findings.
.PHONY: lint-new
lint-new:
	$(GOLANGCI_LINT) run --new-from-rev=$(LINT_BASE_REV)

.PHONY: fmt
fmt:
	$(GOFMT) -w -s $(GOFILES)

.PHONY: imports
imports:
	$(GOIMPORTS) -w $(GOFILES)

.PHONY: test
test:
	GO111MODULE=on $(GO) test -race ./...

.PHONY: release
release:
	./build.bash github.com/jacksontj/promxy/cmd/promxy $(BUILD)
	./build.bash github.com/jacksontj/promxy/cmd/remote_write_exporter $(BUILD)

testlocal-build:
	docker build -t 127.0.0.1:32000/promxy:latest .
	docker push 127.0.0.1:32000/promxy:latest

.PHONY: tidy
tidy:
	GO111MODULE=on $(GO) mod tidy
