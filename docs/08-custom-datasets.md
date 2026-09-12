# Custom datasets — bring your own memories

Every subcommand that takes `--dataset` accepts either a built-in name (`fixtures`, `locomo`,
`longmemeval_s`) or the **path of a JSON file** in the format below. Nothing else changes: the
same ingest, enrichment, snapshot build, search and eval run over your data.

## Format

One file, one or more conversations. A *conversation* is a retrieval scope: questions asked
against it only see its own turns (LoCoMo sample, LongMemEval haystack, one user's memory, one
project channel — whatever you want isolated).

```json
{
  "conversations": [
    {
      "id": "team-standup",
      "turns": [
        {"id": "t1", "session_id": "2024-03-04", "speaker": "maria",
         "text": "I finished the Kafka consumer migration.", "date_time": "2024-03-04 09:05"}
      ],
      "questions": [
        {"id": "q1", "question": "Who migrated the Kafka consumer?",
         "evidence": ["t1"], "question_date": "2024-03-15"}
      ]
    }
  ]
}
```

Field rules (validated at load, with a precise error message):

| Field | Required | Notes |
|---|---|---|
| `conversations[].id` | yes | unique within the file; becomes the conversation name in eval output |
| `turns[].id` | yes | unique within the conversation; what `evidence` refers to |
| `turns[].session_id` | yes | groups turns into sessions; consecutive turns of one session form *rounds* (docs/01) |
| `turns[].speaker` | no | inlined into the indexed text as `speaker: text` |
| `turns[].text` | yes | the memory |
| `turns[].date_time` | no | any string `pipeline.ParseTimestamp` understands (`2024-03-04 09:05`, `2024-03-04`, `1:56 pm on 8 May, 2023`); drives temporal boosting |
| `questions` | no | omit for a search-only corpus; then `eval` reports nothing to score |
| `questions[].evidence` | yes if questions | turn ids of the gold evidence; Recall@k counts a hit when a retrieved turn (or a round containing it) is one of these |
| `questions[].question_date` | no | the "now" for relative dates in the question ("last month") and for the recency boost |

The single-conversation form of `testdata/fixtures.json` (top-level `turns`/`questions`, no
`conversations`) is also accepted; it is treated as one conversation named after the file.

A complete example lives at `testdata/custom-example.json`.

## Where to put the file

`docker-compose.yml` mounts `./datasets` into the server container at `/app/datasets` (read-only,
live — no image rebuild). So:

```bash
mkdir -p datasets
cp ~/my-memories.json datasets/
./scripts/run-snapshot.sh --dataset datasets/my-memories.json --keep-up
```

The client resolves the path as given, then under `/app`, then by base name under
`/app/datasets`, so the same `--dataset datasets/my-memories.json` works from the host shell and
inside the container. The dataset **name** used for ids and the `dataset` metadata key is the
file's base name without extension (`my-memories`), so moving the file does not change the
conversation ids the eval must match.

## Driving the pieces by hand

```bash
make up                                                            # infra + server
docker compose run --rm server /app/migrate up
docker compose run --rm server /app/client ingest --dataset datasets/my-memories.json --version 1
docker compose run --rm server /app/client trigger-sweep
docker compose run --rm server /app/client wait-enriched --version 1 --fail-on-dead
docker compose run --rm server /app/client build-snapshot --version 1
docker compose run --rm server /app/client search --query "who migrated the kafka consumer" \
    --conversation "$(docker compose run --rm server /app/client conv-id --dataset datasets/my-memories.json --id team-standup)"
docker compose run --rm server /app/client eval --dataset datasets/my-memories.json --version 1 --engine snapshot
```

`conv-id` prints the numeric conversation id `search --conversation` expects (ids are a stable
hash of dataset name + conversation id, see `surrogateConvID`). Omit `--conversation` to search
across every conversation in the snapshot.

## Re-ingesting after edits

Memories are content-addressed: re-running `ingest` after editing the file inserts only the
changed/new turns (unchanged ones report as `deduped`), the next sweep enriches only those, and
`build-snapshot` produces a new immutable version. Nothing is ever updated or deleted in Postgres.
