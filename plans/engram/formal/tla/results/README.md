# TLC run logs

Raw TLC output for every configuration under `formal/tla/` (TLC 2.18, Java 21, 4 workers, 10 GB heap, 30-minute
cap), one `<config>.log` per configuration, produced after the round-6 (D25) amendment of the specs (`ShardMove.tla` rewritten for freeze-then-copy, twins in `Derivation.tla`, rebuild and vacuum phases in `Storage.tla`). `RESULTS.md`
summarises them and `../EXPECT` states what each one is expected to do. State-space dumps (`meta_*` directories)
are not kept (the D25 runs were made one configuration at a time; `Outbox`, `Consolidation` and `Durability` are unchanged and their D24 logs stand). A log whose last line reads `elapsed Ns` carries the wall-clock of the whole run (JVM start included);
the table uses TLC's own `Finished in` figure where the run completed. Logs of specs that no longer exist
(`DocLifecycle`, `AsOf`) and of removed configurations (`Derivation_MatInvalid`, the D22 `Derivation_RestoreNoLock`
experiment, which now fails; the D25 deletions `ShardMove_IdKeyedRecopy`, `_MergeNoDeletes`, `_NoSeqAdvanceTwice`,
`_VerifyBeforeCatchUp`, `_NoTimelineCheck`, `_ZeroMargin` and `_CleanupTimeGate`) were deleted.
