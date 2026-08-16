# Setup — Assembling the Working Directory

The design documents were produced as **chat artifacts**, not as downloadable files, which is why they aren't in your `artifacts/` directory. Here's how to assemble a complete working directory before handing it to the agent.

## What you have vs. what you need

| Source | Artifact title in the conversation | Save as |
|---|---|---|
| Chat artifact | Go gRPC Agent-Memory System for LoCoMo and LongMemEval: Embedding-Only Retrieval Design | `docs/01-retrieval.md` |
| Chat artifact | Go gRPC Agent-Memory System with S3 Blobs and Async Postgres-Queue Enrichment | `docs/02-storage.md` |
| Chat artifact | Replacing the Postgres Enrichment Queue with a Temporal Schedule and Sweeper Workflow in Go | `docs/03-temporal.md` |
| Chat artifact | Append-Only Memory Enrichment in Postgres: An Immutable Ledger Design | `docs/04-append-only.md` |
| Downloaded file | `architecture-diagrams` | `docs/05-diagrams.md` |
| Downloaded file | `TESTING` | `docs/06-testing.md` |
| Downloaded file | `docker-compose` | `docker-compose.yml` |
| Downloaded file | `run` | `run.sh` (then `chmod +x`) |
| Downloaded file | `Makefile` | `Makefile` |
| Downloaded file | `main` (the mock embedder) | `tools/mockembedder/main.go` |
| Downloaded file | `append only test` | `internal/store/append_only_test.go` |
| Downloaded file | `AGENT PROMPT` | `AGENT_PROMPT.md` |

The four chat artifacts each have a copy/download control in the artifact panel — grab them from there. Exact filenames don't matter anymore; the updated prompt tells the agent to read every markdown file and identify topics by content, then stop if any of the five required topics is missing.

## Target directory before you start

```
agent-memory/
├── AGENT_PROMPT.md
├── Makefile
├── docker-compose.yml
├── run.sh                                 # chmod +x
├── docs/
│   ├── 01-retrieval.md
│   ├── 02-storage.md
│   ├── 03-temporal.md
│   ├── 04-append-only.md
│   ├── 05-diagrams.md
│   └── 06-testing.md
├── internal/store/append_only_test.go
└── tools/mockembedder/main.go
```

Two paths are load-bearing and must match exactly, because `Makefile` and `docker-compose.yml` reference them: `tools/mockembedder/main.go` (the compose build context) and `internal/store/append_only_test.go` (the `make test-integration` target). Everything else the agent will create.

```bash
mkdir -p agent-memory/{docs,internal/store,tools/mockembedder}
cd agent-memory
# ...move files per the table above...
chmod +x run.sh
```

## Sanity check before handing off

```bash
find . -type f | sort
```

You should see 11 files. If any of the four design docs is missing, the agent will stop at Phase 0 and tell you which topic it can't find — that's intended behavior, not a failure.

## Then

Open your agent in `agent-memory/` and paste the contents of `AGENT_PROMPT.md`.