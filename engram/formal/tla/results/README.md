# TLC results

One log per configuration (`<config>.log`: the raw TLC output followed by one `@@ engram-formal` trailer line that
records the expected outcome, the classified outcome, the verdict and the wall-clock seconds) and `RESULTS.md`, the
table of PLAN.md section 7.6 regenerated from those logs by `scripts/formal-run.sh report`. Both are written by
`scripts/formal-run.sh quick|full`; nothing here is edited by hand. `../EXPECT` states what each configuration must do.
The numbers in `RESULTS.md` come from the machine named in the milestone report (`docs/briefs/reports/M0.7-report.md`)
and differ from the planning-time table of PLAN.md section 7.6 wherever TLC's multi-worker search order differs:
`generated` and `distinct` of a violated configuration are the work done before the counterexample was found, and are
not deterministic with more than one worker.
