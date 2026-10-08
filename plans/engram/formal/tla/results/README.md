# TLC run logs

Raw TLC output for every configuration under `formal/tla/` (TLC 2.18, Java 21, 4 workers, 10 GB heap, 30-minute
cap), one `<config>.log` per configuration. `RESULTS.md` summarises them and `../EXPECT` states what each one is
expected to do. State-space dumps (`meta_*` directories) are not kept. Logs of specs that no longer exist
(`DocLifecycle`, `AsOf`) were deleted with them.
