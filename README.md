# Experiment runner

This repository owns experiment preparation, execution, receipt verification,
request reuse, budgets, provider retries and transport. Project Git worktrees
share this implementation. Project prompts, specs, adapters and generated
experiments remain in their project repositories.

Only `run` sends model requests, and it requires the caller's existing inference
and spending authorization. Moving or installing this tool does not authorize
inference. Tests use local fixtures.

## Use

The source checkout is `~/src/notedeck_worktrees/algos/experiment-runner`.
With the pinned Node dependencies installed in
`experiment_runner/frontier_runner`, its CLI is immediately usable:

```bash
~/src/notedeck_worktrees/algos/experiment-runner/bin/experiment-runner --help
```

Build the immutable package with `nix build`. The package contains the pinned
Python/Node runtime and Node dependencies. It provides the same
`experiment-runner` executable. System installation belongs to `/opt/nixos`;
source changes, releases and tests belong to this repository.

`prepare`, `show`, `revise`, attributed `review` and `reconcile-costs` retain their
existing workflow. Generic cases need no project extension. For Oracle-specific
adapters and commands, use `--project-root` or run inside a checkout containing
`experiment-runner.json`:

```bash
cd ~/src/notedeck_worktrees/algos/oracle-mvp
../experiment-runner/bin/experiment-runner prepare target/example-v1 \
  --spec sse_multivector/experiment_specs/example-v1.json
```

The command is illustrative: supply an existing, authorized spec. Input and
prompt paths resolve relative to the spec. Review and hierarchy extensions are
loaded from the selected project, never copied into this package.

`show <experiment> --inputs-only` prints only each saved case's `id`, exact
`inputs` and `input_sha256`, without reading answer reports or invoking request
previews. `--case` selects one object; `--only` selects a cases list. Evidence,
cost and prompt-only filters cannot be combined with this input-only view.

## Versions and saved experiments

Preparation freezes the exact runner code hashes, model-worker identity,
settings and inputs. The release identifier is available with `--version`.
Source files and installed packages have distinct immutable identities; install
or select the release before preparing new experiments.

Saved receipts remain readable. A prepared experiment cannot silently switch
code or transport. Existing `--runner-upgrade-reason` checks and audit ledgers
remain enforced; a different model-worker identity requires a newly prepared
experiment, normally `prepare --from` the old one. No old receipts or inputs are
rewritten. Failed token usage remains unknown.

`prepare --from` retains the old worker by default, including custom workers.
To move an older experiment's frozen inputs to the shared source worker, prepare
a new directory with `--runner-binary
~/src/notedeck_worktrees/algos/experiment-runner/experiment_runner/run-model.sh`.
This selects the worker only for the new experiment; it does not resume or
rewrite the previous experiment.

Oracle's old Python/shell/Node entry points delegate to this package. For an
explicit source-version pin, set `EXPERIMENT_RUNNER_ROOT` to a checkout of this
repository. A project worktree itself does not select a runner revision.

## Project extensions

An `experiment-runner.json` file declares `schema_version: 1`, an `inputs`
module and an `extension` module. Modules must belong to the selected project.
Input modules implement preparation, rendering, validation and review evidence;
extensions register project commands, collection and views, and declare their
code files. These files are hashed alongside the executor in each prepared plan.
The selected validator is resolved before worker threads start.

## Hierarchy clients and shared attempt bounds

Project constructors can call `prepare`, `report` and `run` with
`include_request_preview=False` to skip request-availability previews. Preparation
still freezes exact inputs and code; execution still validates receipts and
checks exact request reuse. Nonempty `preparation_context` is passed to the
selected project input adapter, which owns its validation.

A constructor can use `PersistentAttemptBudget.initialize(limit)` for its stage
counter and `remaining()` to inspect admission capacity. Initialization preserves
existing usage and deadlines; a changed limit, malformed counter or claims
showing rollback fails instead of resetting the allowance.

`run(..., attempt_budget_path=stage, additional_attempt_budget_paths=(shared,))`
reserves both stage and shared counters before dispatch. Counters are locked in
stable path order, duplicate paths count once, and every counter is validated
before admission. Any exhausted counter or expired optional `deadline_utc`
blocks the request without charging the others. Failed or interrupted launched
attempts are never refunded. A crash during reservation can consume allowance
without launching a call; it cannot launch an unreserved call. Additional
counters must already exist and require a primary counter. These options do not
create a new spending authorization.

Native reports use strict accounting for local attempts and original generations.
They retain `known_catalog_cost_estimate_usd` and
`known_provider_reported_cost_usd` subtotals even when a total is unknown. An
attempt with missing token usage, including a failed response with only SDK zero
placeholders, remains unknown and makes the complete catalog-cost total null.
Verified reused generations are counted once by their original receipt, while
reuse itself adds no new execution cost.

## Verification

```bash
python3 -m unittest discover -s tests
cd experiment_runner/frontier_runner
node --test model.test.mjs auth.test.mjs systemone.test.mjs \
  websocket.test.mjs backoff.test.mjs billing.test.mjs
```

Oracle adapter and compatibility regressions remain in Oracle. The native
cross-process test exercises two experiment processes and the persistent
attempt cap. The credential-service joint fixture verifies a real local
HTTP/2 GOAWAY through the proxy, native SDK, shared admission gate and production
retry policy. No test needs a provider account or teacher request.
