# Agent Guidelines — WALker

## Definition of Done

Before claiming any task complete (marking done, handing off for review, or asserting "all good"):

1. **Tests pass**: `cd walker && go test ./...`
2. **Lint clean**: `cd walker && go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...`
   — or equivalently: `cd walker && make lint`

Both must be green. No exceptions.

## Plans

Write plans as markdown under `docs/plans/YYYY-MM-DD-<short-description>.md`.  
Do not make any code changes until the plan is approved by the user.  
When a plan is implemented, move it to `docs/plans/implemented/`.

## Git

- Do not use interactive rebase.
