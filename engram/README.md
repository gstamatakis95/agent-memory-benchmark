# Engram

A Go + PostgreSQL long-term memory service for AI agents. The design is `docs/plan/PLAN.md` (sections 1–12 and the
binding decision register in Appendix A); code follows the plan, never the other way round. `CONFLICTS.md` records every
place the plan and reality disagree; `PROGRESS.md` tracks the milestones of PLAN.md section 10.

Module `github.com/gstamatakis95/engram`, Go 1.25. Make targets are the tier ladder of PLAN.md section 8.1
(`make lint test-unit`, `make test-prop`, `make test-workflow`, `make test-integration`, `make e2e`, `make e2e-chaos`,
`make formal-quick`, `make formal`, `make gen-docs`; `make test` = S + T0 + T1 fast + T2 + T3).
