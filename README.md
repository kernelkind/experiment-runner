# Experiment runner

This repository owns experiment preparation, execution, receipt verification,
request reuse, budgets, provider retries and transport. Project Git worktrees
share this implementation. Project prompts, specs, adapters and generated
experiments remain in their project repositories.

Only `run` sends model requests, and it requires the caller's existing inference
and spending authorization. Moving or installing this tool does not authorize
inference. Tests use local fixtures.

## Use

Use the shared launcher at
`~/src/notedeck_worktrees/algos/experiment-runner/bin/experiment-runner`.
It loads code directly from the shared `main` checkout.
With the pinned Node dependencies installed in
`experiment_runner/frontier_runner`, its CLI is immediately usable:

```bash
~/src/notedeck_worktrees/algos/experiment-runner/bin/experiment-runner --help
```

Use this launcher for experiment execution. Do not substitute an immutable Nix
package or require a system installation for this workflow.

`experiment-runner --help` lists commands and points to the cache workflow.
`experiment-runner prepare --help` includes the prefix option, an automatic
warming example, and worker migration. `experiment-runner run --help`
explains shared attempt bounds and cache-cost accounting.

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
Preparation records hashes; it does not preserve a separate copy of the runner
code. Later source changes can cause execution checks to refuse a resume.

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

Oracle's old Python/shell/Node entry points delegate to this package.
`EXPERIMENT_RUNNER_ROOT` can select another checkout for compatibility callers;
the shared `main` checkout is the standard. A project worktree itself does not
select a runner revision.

## Update the shared binary after code merges

After **every code change merged into local `main`**, verify that the shared
launcher runs the updated code from that clean checkout. The launcher loads the
merged source directly, so the merge updates its implementation. If pinned Node
dependencies changed, update them with `npm ci --ignore-scripts` in
`experiment_runner/frontier_runner` before verification. Documentation-only and
other non-code changes do not require a binary update.

```bash
cd ~/src/notedeck_worktrees/algos/experiment-runner
bin/experiment-runner --version
bin/experiment-runner --help
bin/experiment-runner prepare --help
```

Verify the shared launcher's relevant behavior and report its stable path and
release version. This workflow uses the checkout launcher, not a command
installed on `PATH`.

Use the updated binary and its worker for newly prepared experiments. Existing
experiments retain their frozen worker and receipts; prepare a new directory
with `--from` and `--runner-binary` to migrate, as described above.

## SSE prefix caching

Version 1.2.0 provides one caching option: `--prompt-cache-prefix FILE`.
It is supported for `openai-codex`, `gpt-6.1-sol` and SSE. Supply a nonempty
UTF-8 file containing the **exact beginning of every rendered prompt**, including
whitespace. Fixed instructions go there; changing case data follows them. The
runner refuses mismatches, derives a stable namespaced key from the prefix's
SHA-256 hash, and freezes both the prefix and key. The same prefix gets the same
key across cases and batches; changing whitespace changes it. The backend still
receives and checks the actual input text; a key does not substitute for it.

```bash
cd ~/src/notedeck_worktrees/algos/oracle-mvp
../experiment-runner/bin/experiment-runner prepare target/compare-cached-v1 \
  --from target/comparisons-v1 \
  --runner-binary ../experiment-runner/experiment_runner/run-model.sh \
  --provider openai-codex --model gpt-6.1-sol --reasoning-effort medium \
  --transport sse --concurrency 8 \
  --prompt-cache-prefix prompts/compare-prefix.txt
../experiment-runner/bin/experiment-runner run target/compare-cached-v1 \
  --max-new-attempts 129
```

Replace the example's experiment and prefix paths with your own. Preparation
makes no model calls. The single `run` command first checks exact-answer reuse.
Immediately before the first comparison requiring a new attempt, it sends one
instructions-only warmup and waits for completion, then runs the comparisons.
This uses the tested developer-prefix/user-data format and native subscription
key/routing headers. Each request remains independent; no previous answer is
included.

Automatic warming requires a prefix with at least **1,024 locally estimated
`o200k_base` tokens** and allowance for warmup plus a comparison. The count covers
only the supplied text, excludes message/schema overhead and is not an exact
subscription tokenizer contract. The documented public API threshold is 1,024
visible tokens ([OpenAI caching guide](https://developers.openai.com/api/docs/guides/prompt-caching));
backend eligibility and cache hits remain unguaranteed. Short
prefixes keep the instruction/data format but skip warming. All-reused or
completed batches skip warming; with only one attempt left, the comparison takes
priority. A resumed invocation with new comparisons may warm again. A failed
warmup stops that invocation before comparisons; a later resume preserves the
old receipts and records any new warmup separately.

Token counting uses Python `tiktoken`; install the declared Python dependencies
in your runtime, or the runner uses its Nix dependency runtime fallback when
`tiktoken` is unavailable. It never substitutes a packaged runner for the shared
source. The local check sends no provider probe.

Every actual attempt, including warmup, retries and failures, consumes the same
`--max-new-attempts` command cap and existing `--attempt-budget PATH` counter.
Programmatic additional counters apply to warmup too. Set your authorized
allowance to include this overhead; do not create/reset counters to warm. The
warmup keeps the comparison schema and model settings, intentionally dispatches
instead of reusing a saved answer, and is preserved in `cache-warmups/` with a
verified parent reference in `cache-warmups.jsonl`. Its answer cannot become a
comparison answer, baseline or hierarchy input.

`show EXPERIMENT --json` and `report.json` include warmup in batch attempt and
usage/cost totals. `cache_warmup` separately shows the frozen eligibility check,
actual warmup attempts, token/cost usage and native receipt references. These
warmup totals cover the experiment even in a filtered case view; warming is
shared overhead. Input, cached input, cache writes, output and reasoning remain
available. Output cost already includes reasoning. A failed call without final
usage leaves complete cost unknown. Raw provider usage stays in immutable
receipts. `--generation-costs` also includes the original warmup once when
tracing multiple answers reused from the same batch.

For specs, set literal `runner.prompt_cache_prefix` text without a manual key.
The CLI option reads and hashes a file. Later `prepare --from` batches inherit
the automatic policy. Omission preserves the previous one-user-message format.
Old manual-key/warmup experiments retain their frozen behavior; the legacy
options remain accepted for compatibility but are hidden from normal help.
Supply the prefix option again to opt an older experiment into the automatic
workflow. Existing worker identity checks still require selecting the shared
worker for a newly prepared experiment. `prepare --repeat` requests independent
fresh trials; otherwise identical comparisons reuse saved answers. WebSocket
cache options are refused before dispatch.

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
