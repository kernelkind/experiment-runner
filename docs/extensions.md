# Project extensions

[Project overview](../README.md) · [Running experiments](running-experiments.md)

The built-in `generic` adapter accepts prompt templates and string-valued case
variables without another project. An extension lets a project prepare its own
input formats, validate answers, supply review interfaces and register commands.
The shared runner continues to execute requests and record their evidence.

## Selecting a project

Run the CLI from a project with `experiment-runner.json`, or select its root with
the global `--project-root PATH` option before the command. Automatic discovery
walks the current directory and parents, stopping at the nearest project
configuration or Git root. A configuration declares its project-owned modules:

```json
{
  "schema_version": 1,
  "inputs": "my_project.inputs",
  "extension": "my_project.extension"
}
```

The module names above are placeholders for your implementation. Both modules
must load from the selected checkout. Merely pointing to a project does not add
its code to this package.

The spec still supplies `prompt`, `dataset` and `runner`. The shared parser
currently recognizes the dataset keys `adapter`, `path`, `select`, `mask_format`,
`combine_masks` and `prominence_basis`; adapters validate their supported subset.
The interface does not accept arbitrary new dataset keys.

## Extension interfaces

An `experiment-runner.json` file declares `schema_version: 1`, an `inputs`
module and an `extension` module. Modules must belong to the selected project.
Input modules implement preparation, rendering, validation and review evidence;
extensions register project commands, collection and views, and declare their
code files. These files are hashed alongside the executor in each prepared plan.
The selected validator is resolved before worker threads start.

### Mechanical results

A prepared case can include an adapter-provided mechanical result:

```json
{
  "mechanical": {
    "output": "{\"label\":\"Gardening\"}",
    "evidence": {"rule": "exact_lookup", "key": "gardening"}
  }
}
```

The generic adapter also accepts this field in its source cases. Adapters own
the rule and source evidence; the runner adds no project-specific semantic
rules. Only `output` and a nonempty evidence object are accepted. Preparation
rejects malformed JSON, duplicate output keys and nonfinite JSON constants,
then passes the output through the selected adapter's normal validator. Schema
and semantic validation remain that adapter's responsibility; generic validation
parses JSON without independently enforcing JSON Schema constraints.

The complete metadata is bound by the prepared plan's digest. `prepare` and
`show` expose validated results immediately with `origin: "mechanical"`,
`mechanical_evidence`, `mechanical_sha256` and `attempts: 0`. Their native summary
includes a `mechanical` count. These cases have no model receipt and add no
attempts, token usage, generation costs or cache hits. Attributed reviews bind
their input and output digests normally. A mechanical result does not establish
model qualification.

`run` filters mechanical cases from dispatch while retaining the original
prompts, config and manifest. An all-mechanical batch requires no model worker
execution, warming or attempt-budget admission. Mixed batches execute and account
for only residual requests and any eligible warmup. Execution code provenance
checks still apply. Explicit `prepare --repeat` disables mechanical routing and
executes the normal prompts as fresh model trials. Cases without this metadata
retain the existing request, receipt, import and `--from` behavior.

### Prompt sources and input inspection

A project extension may implement `prompt_sources(prompt)`. Preparation calls
it for a spec's prompt and for an explicit `--prompt` replacement, before
creating the saved experiment. The callback receives `id`, `text` and `source_path`;
it validates project-owned composition and returns absolute source paths mapped
to SHA-256 hashes. The runner verifies those hashes and freezes them alongside
the prompt and input sources. A rejection makes no model calls and creates no
experiment. Existing projects without the hook retain their behavior. An
unchanged `prepare --from` keeps the original frozen prompt and dependencies.
This supports shared prompt components without putting project rules in the
executor or rewriting historical prompts.

`show <experiment> --inputs-only` prints only each saved case's `id`, exact
`inputs` and `input_sha256`, without reading answer reports or invoking request
previews. `--case` selects one object; `--only` selects a cases list. Evidence,
cost and prompt-only filters cannot be combined with this input-only view.

The runner calls `code_files(dataset)`, `add_commands(commands)` and
`enrich_report(result, plan, cases)` on a selected extension. For CLI dispatch,
`handle_command(args)` returns `NotImplemented` to let the core handle a command.
The optional `prompt_sources` hook is described above. `review` and `collect`
delegate to the project's implementation when selected; a review UI requires
that implementation. See [project.py](../experiment_runner/project.py) and
[experiment.py](../experiment_runner/experiment.py) for the current call contract.

## Python callers and shared attempt bounds

Project callers can call `prepare`, `report` and `run` with
`include_request_preview=False` to skip request-availability previews. Preparation
still freezes exact inputs and code; execution still validates receipts and
checks exact request reuse. Nonempty `preparation_context` is passed to the
selected project input adapter, which owns its validation.

A caller can use `PersistentAttemptBudget.initialize(limit)` for its stage
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
counters must already exist and require a primary counter. These counters track
attempts; they are not dollar spending limits.

Native reports use strict accounting for local attempts and original generations.
They retain `known_catalog_cost_estimate_usd` and
`known_provider_reported_cost_usd` subtotals even when a total is unknown. An
attempt with missing token usage, including a failed response with only SDK zero
placeholders, remains unknown and makes the complete catalog-cost total null.
Verified reused generations are counted once by their original receipt, while
reuse itself adds no new execution cost.

## Selecting and warming request slices

Programmatic `request_preview(..., include_reusable_case_ids=True)` adds
`reusable_case_ids` for unrun cases whose saved receipts and outputs pass the
existing reuse checks. The default preview still returns counts only. Passing
those IDs to `run(..., only=ids, max_new_attempts=0)` resolves saved answers even
when an earlier case needs a new call. Execution rechecks the receipts.

Programmatic `run(..., warm_cache=True)` retains automatic warming by default.
A caller running several selected slices for one logical invocation can use
`warm_cache=False` after that group's initial slice, so later slices share its
warmup. This changes neither the frozen cache identity nor comparison requests.
Dispatched slices record `only` and `warm_cache` in `execution-settings.jsonl`.

## Oracle MVP

Oracle MVP uses the runner for subject/mask prompting and hierarchy construction.
Its `experiment-runner.json` selects `sse_multivector.experiment_inputs` and
`sse_multivector.experiment_extension` from the Oracle checkout.

| Responsibility | Location |
| --- | --- |
| Generic prompt preparation/execution, receipts, retries, answer reuse and accounting | Experiment Runner |
| Oracle research prompts, experiment specs, hierarchy algorithms, specialized input adapters and review interfaces | Oracle MVP |
| Subject, mask and prominence extraction workflows and the word-position mask decoder | Currently retained in Experiment Runner's `extraction_pipeline.py` and `mask_positions.py`; Oracle entry points delegate to them. |
| Oracle hierarchy dispatch and specialized report/review handling | Some branches remain in the shared CLI/report implementation. |

The runner is usable independently, but this is not a complete architectural
separation. In particular:

- `run` and `show` recognize directories containing `bounded-build.json` and
  load the configured project's `bounded_hierarchy_build` module. Reading or
  resuming Oracle's bounded builds requires the Oracle project extension.
- The shared report code recognizes adapters such as `masks`, `prominence` and
  `bounded_comparisons`, including Oracle review comments and UI evidence.
- Some review flags and dataset options refer to Oracle's formats. Another
  extension does not receive an entirely project-neutral parser.
- Internal worker identifiers and the default private request-queue cache path
  retain Oracle names. These names alone do not require an Oracle checkout.

Normal generic prompt experiments use the built-in path and need no Oracle
modules. Authentication is a separate choice: direct provider access works
without Anvil; the default route uses its account service. See
[authentication](running-experiments.md#providers-and-authentication).

Oracle's hierarchy coordinator currently expects the runner's source-checkout
layout. Oracle's own guide supplies its additional dependencies and commands.
The extension mechanism does not make those hierarchy commands available to a
standalone installation without Oracle.
