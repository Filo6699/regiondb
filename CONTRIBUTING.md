# Contributing to regiondb

Keep each change focused on one coherent behavior or documentation concern.
Prefer small commits that can be reviewed, tested, and reverted independently.
Do not combine unrelated cleanup with a functional change.

Commit subjects must follow Conventional Commits:

```text
type(optional-scope): concise imperative summary
```

Use a specific type such as `feat`, `fix`, `test`, `docs`, `perf`, `refactor`,
`build`, or `ci`. The committed tree must build and pass the checks relevant to
the change.

## Scope

Implement only behavior needed by the change under review. Do not add
placeholder APIs, commands, flags, formats, metrics, compatibility statements,
or documentation for possible future work. Follow-up work belongs in a
separate change when its behavior and contract are ready for review.

Tests must cover existing behavior or behavior introduced by the same change.
Refactoring belongs in the same commit only when it is a minimal prerequisite;
otherwise, submit it separately.

## Experimental features

Experimental features must be explicitly opt-in and must not replace a
production default. Their formats and behavior carry no support, migration,
compatibility, durability, or long-term availability guarantee unless a later
stable contract states otherwise.

A contribution touching an experiment must distinguish measured observations
from guarantees and must describe the production behavior that remains
unchanged. Performance evidence does not establish correctness or production
readiness.

## Validation

Run the local checks that cover the changed surface. The baseline Go gate is:

```sh
test -z "$(gofmt -l .)"
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet ./...
go test ./...
go build ./...
```

Storage, recovery, protocol, concurrency, and performance changes require their
focused suites in addition to the baseline gate.
