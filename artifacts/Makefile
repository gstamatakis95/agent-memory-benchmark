.PHONY: help proto test test-unit test-workflow test-integration e2e e2e-chaos eval up down logs

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

proto: ## regenerate protobuf/gRPC stubs
	protoc -I proto \
		--go_out=. --go_opt=module=example.com/agentmem \
		--go-grpc_out=. --go-grpc_opt=module=example.com/agentmem \
		proto/agentmem/v1/*.proto proto/embed/v1/*.proto

test: test-unit test-workflow test-integration ## tiers 0-2

test-unit: ## tier 0: pure logic, no infra (<1s)
	go test -race -short ./internal/retrieve/... ./internal/pipeline/... ./internal/eval/...

test-workflow: ## tier 1: Temporal in-memory test env, no server
	go test -race ./internal/enrich/...

test-integration: ## tier 2: Postgres invariants via testcontainers
	go test -race -tags=integration ./internal/store/...

e2e: ## tier 3: full stack on fixtures, asserts R@5 == 1.0
	./run.sh --fixtures

e2e-chaos: ## tier 3 with fault injection: outage + flaky embedder + bad dims
	MOCK_FAIL_RATE=0.1 MOCK_FAIL_UNTIL=45s MOCK_BAD_DIMS_RATE=0.01 ./run.sh --fixtures

eval: ## tier 4: real benchmark. usage: make eval DATASET=longmemeval_s RETRIEVAL=hybrid
	./run.sh --dataset $(or $(DATASET),locomo) --retrieval $(or $(RETRIEVAL),hybrid) --keep-up

# The prefix sanity gate. If hybrid does not beat bm25 by ~+9pp on LongMemEval-S,
# the embedding path is broken (missing search_document:/search_query: prefix,
# missing L2 normalization, or a dimension mismatch) -- fix that before tuning.
ablation: ## run bm25 / dense / hybrid back to back and compare
	@for m in bm25 dense hybrid; do \
		echo "=== $$m ==="; \
		./run.sh --dataset longmemeval_s --retrieval $$m --keep-up; \
	done

up: ## bring the stack up without running eval
	docker compose up -d --wait

down:
	docker compose down -v

logs:
	docker compose logs -f server embedder-mock
