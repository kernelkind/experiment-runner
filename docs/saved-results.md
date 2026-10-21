# Saved results

[Project overview and tutorial](../README.md) · [Running experiments](running-experiments.md)

The runner keeps the prepared experiment, its execution evidence, current
reports and compact exports as distinct files.

## Experiment files

| Path | Contents |
| --- | --- |
| `experiment.json` | Frozen prompt, cases, settings, source/code hashes and any baseline. |
| `runner-config.json` | Prepared model-worker configuration. |
| `report.json` | Latest saved case outcomes, usage and comparisons. |
| `reviews.jsonl` | Attributed case judgements, when recorded. |
| `run/` | Request manifest, response receipts, attempts and execution records. |
| `publication/` | Compact inputs, final answers, settings and reviews for standalone experiments. |

Preparation writes a `.gitignore` inside each generated experiment directory.
Keep reusable prompts, case sources and specs outside those generated directories.
Code hashes identify the implementation; they do not bundle its source. Retain
the corresponding code revision as well.

## Inspect and review

```bash
cd ~/experiment-runner-example
experiment-runner show run-v2
experiment-runner show run-v2 --case gardening
experiment-runner show run-v2 --inputs-only
```

The summary includes case states, comparisons, requests remaining and usage.
The case view contains `inputs`, the rendered `prompt`, `output`, available
receipt evidence, any `baseline`, and reviews. Add `--json` for a machine-readable
summary; a case view is already JSON. `--prompt-only` prints its rendered prompt.

Comparison labels include `changed`, `unchanged`, `missing_baseline`,
`input_mismatch` and `not_comparable`. They describe saved text and comparability,
not measured answer quality. Generic plain-text validation applies no semantic
correctness rule. Project adapters can add validation and review views.

`review` records an `agent` or `user` judgement with verdict `accept`, `reject` or
`uncertain`, plus a comment. Reviews bind input/output hashes, so a review of a
different answer is not presented as current. A recorded judgement is separate
from validation and from any model qualification claim.

## Cost reporting

`usage.cost_statistics` is available in native `report.json`, `show --json`
and `--generation-costs` reports. Readable `run` and batch `show`
output print total, mean, median and p95 request cost, comparison cache-hit rate,
and estimated net dollars/percentage saved. `run --help` and `show --help`
explain the calculation. No extra model request is used for reporting.

Costs use the saved SDK catalog estimates, including cache reads, cache writes
and output. They are API-equivalent estimates, not subscription charges or quota
measurements. New model receipts save `uncached_catalog_cost_estimate_usd` with
the same catalog and full-request pricing tier used by the original call. The
counterfactual prices all observed input at the uncached rate and keeps the same
output, which already includes reasoning. Reports never reprice old calls using
today's catalog. For older receipts, the saved uncached-input cost component can
establish their input rate; ambiguous legacy input scope or an unavailable rate
leaves savings unknown.

For a batch, let **U** be the sum of uncached estimates for comparison attempts,
and **C** its saved actual catalog cost, including warmup and all retries:

```text
net_cache_savings_usd = U - C
net_cache_savings_percent = 100 * (U - C) / U
```

Warmup contributes to **C**, not **U**. A tiny batch can therefore have negative
savings even when every comparison hits the cache. Cache-hit rate counts
comparison attempts reporting positive cached input; warmup and exact-answer
reuse are excluded. Per-request distributions include every actual attempt,
including warmup and retries. P95 uses nearest rank. Reused answers add zero new
attempts/cost; `--generation-costs` reports their unique original attempts and
original warmup instead.

These figures estimate caching's price effect on the **observed tokens**. They
do not measure how a separate uncached run would change outputs, token counts,
failures or latency. Use a controlled experiment for that comparison.

Missing usage or cost keeps the complete total/distribution and net savings
unknown; existing known cost subtotals remain available. Missing historical
pricing can leave savings unknown while actual-cost statistics remain known.
`requests_without_cost`, `comparison_requests_without_usage` and
`comparison_requests_without_uncached_cost` show coverage.
An empty request set has zero total cost, but no mean/median/p95, hit rate or
percentage saved. A zero uncached denominator also has no defined savings
percentage. Filters retain the experiment's shared warmup overhead as described
in the [prefix-caching guide](running-experiments.md#sse-prefix-caching), without
allocating it between cases.

## Compact publication results

Standalone prompt experiments save a managed `publication/` directory during
preparation, execution and review. It contains a small `experiment.json` with
settings, version/source bindings and payload checksums; deduplicated template
variables in `inputs.jsonl`; and final case answers, outcomes, reuse references
and attributed reviews in `results.jsonl`. Rendered prompts, recursive baseline
reports, transport details and arbitrary evidence wrappers are excluded.

In an agent review, exports omit the parenthetical session UUID from a leading
`Agent review by REVIEWER (session UUID):` attribution. The reviewer label and
scientific assessment remain, along with the recorded review fields and
source-report hash. User comments and scientific identifiers are preserved.

`export-results` exports the same format from an explicitly selected saved
experiment without inference. It streams the saved report and binds its input
selection and hash; it does not reconstruct a missing report, merge newer review
sidecars or perform receipt verification. Metadata states these limitations.

`prepare --publication-role standalone|internal` freezes whether a job owns a
public result. Normal preparation defaults to standalone; project coordinators
mark construction jobs internal, and cache warmups are always internal.
`prepare --from` inherits the role. Unmarked historical plans stay unmarked unless
the caller explicitly selects a role for the new preparation. Automatic export
requires standalone; known internal jobs cannot be exported separately.

The shared standard-library `publication_files` writer stages streamed JSONL
payloads, rejects unrelated/different-owner destinations, binds payload hashes
and replaces metadata last. Unchanged exports keep bytes and modification times.
Project evaluation and numerical adapters can use it without a transport or model
dependency. Choosing public datasets, reviewing sensitive content and uploading
the files remain separate work.

## Verify native evidence

Extract the evidence package with its directory structure intact, then pass the
experiment directory to `verify`:

```bash
cd ~/experiment-runner-example
experiment-runner verify release/evidence/project/trial
```

The command finds the nearest `experiment-locations.json` in the experiment's
parent directories. Publishers include this file when packaging relocated native
evidence; readers can use it without editing saved records. Its destinations
must stay inside the extraction root. Missing or invalid bundled entries fail
without falling back to the original machine.
For an experiment still at its original location, no metadata is needed.

`verify` checks the frozen plan, prepared-file and source hashes, manifest,
ledger sequence and provenance, and every successful response receipt. Reused
responses are checked against their original producer receipts. It retains the
original path strings and hashes, makes no model calls or evidence changes, and
requires neither credentials nor an installed model worker.

Add `--json` for the plan hash, checked-source and receipt counts, and latest
case statuses. Exit status zero means the integrity checks passed, even if some
cases failed or never ran. Failed attempts receive ledger provenance/sequence
checks, not successful-response checks. Mechanical decisions and answer quality
are not reevaluated.

Check archive checksums first: internal hashes do not establish a collection's
completeness or authenticity. Verification does not reconstruct aggregates,
verify all transitive adapter evidence, recover historical code, or evaluate
review notes, reconciled costs or whole hierarchy/training outputs. It streams
source hashes and receipt ledgers, but loads the selected plan, configuration
and manifests as JSON. Verify individual child experiments for a large build.
Bundled locations apply only to `verify`; `run`, `show`, `collect` and
`prepare --from` keep their existing path behavior.

### Prepare locations for publication

Publishers stage the selected experiments and dependencies before making an
archive. Include bound historical sources and the original receipts, manifests
and upgrade ledgers needed by reused answers. Preserve the frozen bytes.

Keep the original-to-staged locations used during copying in a private file
outside the archive. For example, `locations-private.json` beside `release/`:

```json
{
  "schema_version": 1,
  "roots": {"/old/project": "release/evidence/project"},
  "files": {"/old/project/posts.json": "release/datasets/posts.json"}
}
```

Destinations in this private file are relative to the file, or absolute. Exact
file entries override roots; the longest matching root wins. This allows a
corpus to occupy a shared dataset location. Use the actual paths from staging.
Credentials and the old runner executable are not verification dependencies.

```bash
cd ~/experiment-runner-example
experiment-runner prepare-release release \
  --locations locations-private.json \
  --experiment evidence/project/trial
```

Repeat `--experiment` for all selected experiments sharing the extraction root.
The command verifies the staged evidence, sharing source hashes across those
experiments, then writes `release/experiment-locations.json`. It includes only
used bindings, with relative destinations inside that root. External
dependencies and changed or missing evidence fail before metadata is written.
Existing metadata is never overwritten. A successful check can still include
failed and unrun cases; it does not make them successful experiments.

Include the generated file in the archive and its checksums; keep the private
staging file outside it. Split archives sharing an extraction root must supply
the same complete metadata, rather than overwriting it with partial mappings.
Test after extraction elsewhere, without `--locations` or original storage.
This command prepares metadata; archive selection, copying and compression
remain publisher steps. Explicit `verify --locations FILE` remains available
for publisher checks and overrides automatic lookup.
