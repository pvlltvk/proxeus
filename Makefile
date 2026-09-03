BUILD := build
GO ?= go
GOFILES := $(shell find . -name "*.go" -type f)
GOFMT ?= gofmt
GOIMPORTS ?= goimports -local=github.com/pvlltvk/proxeus
STATICCHECK ?= staticcheck
GOLANGCI_LINT ?= golangci-lint
# Revision new code is compared against in `make lint-new`.
LINT_BASE_REV ?= origin/main

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

# Synthetic Prometheus backend used to load-test proxeus without a real
# Thanos/VictoriaMetrics behind it. Deliberately not part of `release`: it is a
# test tool, not a published artifact.
.PHONY: fakeprom
fakeprom:
	$(GO) build -tags netgo -o $(BUILD)/fakeprom github.com/pvlltvk/proxeus/cmd/fakeprom

# End-to-end query benchmarks: proxeus in front of fakeprom backends. Tune the
# sweep with the PROXEUS_BENCH_* env vars documented in
# test/fakeprom_bench_test.go; set BENCH_PROFILE_DIR to collect profiles.
BENCHTIME ?= 3x
BENCH_PROFILE_DIR ?=
.PHONY: bench-e2e
bench-e2e:
	@if [ -n "$(BENCH_PROFILE_DIR)" ]; then mkdir -p $(BENCH_PROFILE_DIR); fi
	GO111MODULE=on $(GO) test ./test/ -run='^$$' -bench=BenchmarkFakepromQueryRange -benchmem -benchtime=$(BENCHTIME) -timeout=60m \
		$(if $(BENCH_PROFILE_DIR),-cpuprofile=$(BENCH_PROFILE_DIR)/cpu.pprof -memprofile=$(BENCH_PROFILE_DIR)/mem.pprof)

.PHONY: release
release:
	./build.bash github.com/pvlltvk/proxeus/cmd/proxeus $(BUILD)
	./build.bash github.com/pvlltvk/proxeus/cmd/remote_write_exporter $(BUILD)

testlocal-build:
	docker build -t 127.0.0.1:32000/proxeus:latest .
	docker push 127.0.0.1:32000/proxeus:latest

.PHONY: tidy
tidy:
	GO111MODULE=on $(GO) mod tidy
