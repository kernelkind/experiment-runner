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

## SSE prefix caching

Version 1.1.0 adds opt-in prefix caching for the `openai-codex` subscription
route, with `gpt-6.1-sol` and SSE. The tested configuration sends fixed
instructions in a developer input item, changing case data in a separate user
item, and a stable cache key in the body and routing headers. Each request is
independent: earlier answers are never included. It uses the existing admission,
retry, receipt-validation and exact-answer-reuse paths.

Prepare a nonempty UTF-8 file containing the **exact beginning of every rendered
prompt**, including spaces and line endings. Put fixed instructions there and
changing case data after it. The runner refuses a prefix mismatch. Choose a
stable key of 1–64 visible ASCII characters without spaces; reuse it for the
same fixed instructions across the warmup and comparison batches. Do not
generate a new key for each case or directory.

The following workflow assumes an existing comparison experiment
`target/comparisons-v1` and its fixed prefix in `prompts/compare-prefix.txt`.
Replace those two paths with your experiment and prefix file. Run from your
project checkout. Building the package makes no model calls; the two `run`
commands use your existing inference authorization. Set the attempt bounds to
your allowance, including warmup and possible retries.

```bash
cd ~/src/notedeck_worktrees/algos/oracle-mvp
runner_package="$(nix build ../experiment-runner --no-link --print-out-paths)"
"$runner_package/bin/experiment-runner" prepare target/compare-cached-v1 \
  --from target/comparisons-v1 \
  --runner-binary "$runner_package/lib/experiment-runner/experiment_runner/run-model.sh" \
  --provider openai-codex --model gpt-6.1-sol --reasoning-effort medium \
  --transport sse --concurrency 8 \
  --prompt-cache-key hierarchy-compare-v1 \
  --prompt-cache-prefix prompts/compare-prefix.txt
"$runner_package/bin/experiment-runner" prepare target/compare-warmup-v1 \
  --from target/compare-cached-v1 --cache-warmup
"$runner_package/bin/experiment-runner" run target/compare-warmup-v1 \
  --max-new-attempts 1
"$runner_package/bin/experiment-runner" run target/compare-cached-v1 \
  --max-new-attempts 128
```

The warmup is one ordinary instructions-only inference request, prepared at
concurrency one. It keeps the comparison schema and key, intentionally sends
again instead of reusing a saved answer, and records its own usage. Its answer
does not become a comparison answer, baseline or hierarchy input. Finish the
warmup before running the comparison batch. Prepare another uniquely named
warmup with `--from` that batch when intentionally warming a later batch;
do not reset existing receipts or budgets. Warmup preparation rejects runner
setting overrides so its key and model settings match that comparison batch.

If you already use a persistent allowance, pass
`--attempt-budget target/attempt-budget.json` to **both** `run` commands, using
the existing counter. Every actual attempt, including warmup, retries and
failures, consumes it. `--max-new-attempts` bounds each command; the persistent
counter bounds their combined work. Keep the selected subscription, model,
schema, prefix and key stable for the comparison. Later batches prepared with
`--from` inherit the prefix/key; a new spec can set the same options explicitly.

For specs, `runner.prompt_cache_key` is the key string and
`runner.prompt_cache_prefix` is the literal prefix text. The CLI's
`--prompt-cache-prefix` instead reads a file and freezes its bytes and hash.
Omitting these settings keeps the previous one-user-message format. Existing
experiments retain their frozen worker; select the new packaged worker when
preparing a new experiment as above. Intentionally independent repeat trials
require `prepare --repeat`; ordinary identical comparisons reuse saved answers.
Cache options with WebSocket are refused before dispatch.

Read usage through `show EXPERIMENT --json` or its saved `report.json`: input,
cached input, cache writes, output/reasoning and known catalog-cost subtotal are
retained. Add warmup and comparison costs when measuring savings. Reasoning is
already part of output cost. A failed call without final usage keeps the total
unknown; zero SDK placeholders are not evidence of a free attempt. Raw provider
usage remains in immutable model receipts.

The completed 1,032-attempt, eight-block comparison measured **13.0659% lower
total API-equivalent cost**, including eight warmups, with no failures or
retries. Every block was cheaper; the exact balanced-randomization p-value was
1/70. Cache reads occurred in 128 of 512 candidate comparisons. Three of 512
matched pairs differed on one J-tubes/Food preparation case, with variation in
both formats and unchanged supplied meanings. The user accepted the technique's
usefulness; this is bounded workload evidence, not semantic certification or a
guaranteed hit rate. The configuration uses no public-API explicit cache options
or breakpoint fields. Frozen study evidence is preserved in the Oracle worktree
`oracle-mvp-canonical-cache-controls-v1/target/canonical-cache-sse-controlled-v4/`.

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
