# TLC run logs

Raw TLC output for every configuration under `formal/tla/` (TLC 2.18, Java 21, 4 workers, 10 GB heap, 30-minute
cap), one `<config>.log` per configuration, produced after the round-5 (D24) amendment of the specs. `RESULTS.md`
summarises them and `../EXPECT` states what each one is expected to do. State-space dumps (`meta_*` directories)
are not kept (the D24 runs were made one configuration at a time). A log whose last line reads `elapsed Ns` carries the wall-clock of the whole run (JVM start included);
the table uses TLC's own `Finished in` figure where the run completed. Logs of specs that no longer exist
(`DocLifecycle`, `AsOf`) and of removed configurations (`Derivation_MatInvalid`, the D22 `Derivation_RestoreNoLock`
experiment, which now fails) were deleted.
