# TLC run logs

Raw TLC output for every configuration under `formal/tla/` (TLC 2.18, Java 21, 4 workers, 10 GB heap, 30-minute cap),
one `<config>.log` per configuration, produced after the round-7 (D26) amendment of `ShardMove.tla` and `Derivation.tla`;
the logs of `Outbox`, `Consolidation`, `Storage` and `Durability` are unchanged. `RESULTS.md` summarises them and
`../EXPECT` states what each one is expected to do. State-space dumps (`meta_*`) are not kept. A log whose last line reads
`elapsed Ns` carries the wall-clock of the whole run (JVM start included); the table uses TLC's own `Finished in` figure.
Logs of removed configurations are deleted.
