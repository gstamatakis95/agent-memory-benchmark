# Make targets are exactly the ones PLAN.md section 8.1 names. Each chain refines only the recipe of the targets it
# owns; the target names are frozen. `make test` = S + T0 + T1 (fast) + T2 + T3.
SHELL := /bin/bash
GO ?= go
GOBIN ?= $(shell $(GO) env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)
PKGS ?= ./...

.PHONY: lint test-unit test-prop test-prop-deep test-workflow test-integration e2e e2e-chaos bench-smoke bench-pg \
        formal-quick formal gen-docs test proto

lint:
	cd proto && buf lint && buf build && buf format -d --exit-code
	$(GO) vet -tags integration,faultinject,testauth $(PKGS)
	golangci-lint run $(PKGS)
	$(GO) run ./scripts/methodcount ./internal/... ./adapters/...
	scripts/check-line-length.sh

proto:
	cd proto && buf generate

test-unit:
	$(GO) test -short -count=1 $(PKGS)

# Only packages whose tests import pgregory.net/rapid accept the -rapid.checks flag.
RAPID_PKGS = $(shell $(GO) list -f '{{.ImportPath}} {{join .TestImports " "}} {{join .XTestImports " "}}' ./... \
	| grep -F pgregory.net/rapid | cut -d' ' -f1)

test-prop:
	@if [ -z "$(RAPID_PKGS)" ]; then echo "test-prop: no package imports pgregory.net/rapid yet"; \
	else $(GO) test -count=1 $(RAPID_PKGS) -rapid.checks=200; fi

test-prop-deep:
	@if [ -z "$(RAPID_PKGS)" ]; then echo "test-prop-deep: no package imports pgregory.net/rapid yet"; \
	else $(GO) test -count=1 -timeout 60m $(RAPID_PKGS) -rapid.checks=20000; fi

test-workflow:
	$(GO) test -count=1 ./internal/workflows/...

test-integration:
	$(GO) test -count=1 -p 4 -tags integration -timeout 20m $(PKGS)

e2e:
	scripts/e2e.sh

e2e-chaos:
	scripts/e2e-chaos.sh

bench-smoke:
	scripts/bench-smoke.sh

bench-pg:
	scripts/bench-pg.sh

formal-quick:
	scripts/formal-run.sh quick

formal:
	scripts/formal-run.sh full

gen-docs:
	scripts/gen-docs.sh

test: lint test-unit test-prop test-workflow test-integration
