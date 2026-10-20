# Development

[Project overview](../README.md) · [Project extensions](extensions.md)

This guide covers source launchers, testing and execution internals. The
[experiment guide](running-experiments.md) covers ordinary CLI use.

## Tests

```bash
cd "$EXPERIMENT_RUNNER_ROOT"
source .venv/bin/activate
python3 -m unittest discover -s tests
cd experiment_runner/frontier_runner
npm test
```

Oracle adapter and compatibility regressions remain in Oracle. The native
cross-process test exercises two experiment processes and the persistent
attempt cap. The credential-service joint fixture verifies a real local
HTTP/2 GOAWAY through the proxy, native SDK, shared admission gate and production
retry policy. No test needs a provider account or a paid model request.
Direct-authentication fixtures exercise pi-ai device login, cross-process token
refresh, provider failures and a native prepare/run/resume cycle using local
servers. These are offline integration checks, not live-provider validation.

## Source launchers

The installed `experiment-runner` command and the source `bin/experiment-runner`
launcher expose the same CLI. The source launcher loads its checkout directly.
`EXPERIMENT_RUNNER_ROOT` can explicitly select another source checkout for that
launcher and Oracle compatibility callers; an invalid explicit root fails
without fallback. A project worktree alone does not select a runner revision.

When changing pinned Node dependencies, reinstall them:

```bash
cd "$EXPERIMENT_RUNNER_ROOT"
npm --prefix experiment_runner/frontier_runner ci \
  --ignore-scripts --no-audit --no-fund
```

Source-launcher changes need no separate binary build. Python packaging follows
`pyproject.toml`.

Keep bound source and dependencies stable during execution. Existing Python
coordinators retain their loaded code; installing an update does not replace
already-running Python processes. Use the per-experiment pause/resume workflow
in [pausing and upgrading](running-experiments.md#pausing-and-upgrading).

## Implementation layout

| Module | Responsibility |
| --- | --- |
| `experiment.py` | Preparation, saved reports, CLI workflow, reviews and extension dispatch. |
| `extraction_pipeline.py` | Model execution, receipt checks, history and budgets, alongside retained Oracle extraction workflows. |
| `project.py` / `inputs.py` | Project discovery/extension selection and built-in generic inputs. |
| `scheduling.py` / `provider_backoff.py` | Host-local admission, fairness and provider recovery. |
| `frontier_runner/` | Pinned pi-ai transport, authentication and provider request/response handling. |
| `compatibility.py` | Request compatibility and append-only worker provenance. |
| `verification.py` | Native evidence checks and relocated-file bindings. |
| `prompt_publication.py` / `publication_files.py` | Compact saved prompt exports and shared publication writing. |

## Worker admission

Waiting cases stay in the runner process; model workers start only after
admission. Native workers advertise `experiment_scopes: true` in `--identity`
and implement `scopes`: service mode queries the local credential service;
direct mode resolves pi-ai credentials and computes the account/transport hashes
locally. Both return only hashes, without inference. The runner discovers scopes
once per batch and the worker
checks its admission against the selected scopes again before sending. Scope
failure stops dispatch; changed scopes refuse the request. Custom workers
without this capability still share the 30 slots. They do not gain native
account/transport discovery by declaring a provider name.

A worker can also advertise `endpoint_admission: true` in `--identity` and
implement the read-only `admission` command. It returns a hashed endpoint
`scope` and the endpoint's fixed integer `capacity` (1 through 30), without
inference. The runner discovers this metadata once per batch and enforces that
capacity across experiments using the same endpoint in the existing shared
queue. A serial local worker reports capacity one. Endpoint waiting happens
before worker launch, attempt reservation and the active-request timeout;
distinct endpoints and provider requests remain eligible for global slots.
Malformed or unavailable advertised metadata refuses dispatch. Endpoint hashes
stay in the private admission state; operational history and status expose only
counts and the existing `capacity` wait reason. There is no public capacity or
concurrency override.

The queue uses the existing private provider-backoff state under
`$XDG_CACHE_HOME/oracle-mvp/provider-backoff` (normally `~/.cache/...`). Process
identity and request deadlines release stale admissions after cancellation or
exit. A live orphaned model worker retains its slot even after its runner exits.
A resumed run validates saved receipts and budgets normally. Live state
is operational evidence, separate from immutable model receipts. The limit
covers this user's cooperating runner executions on this host; it is not the
provider's concurrency contract or a reservation guaranteeing chat capacity.

## Scheduling history and diagnostics

Each run retains append-only operational history at
`EXPERIMENT/run/scheduling.jsonl` (next to `scheduling.json`). Observations cover
startup, readiness, admission, release/cancellation, prefix-warmup boundaries,
batch backoff, periodic progress and completion. Each row captures
`observed_at_utc`, `monotonic_seconds`, `boot_id`, occupancy, ready counts and
the batch's blocking reasons from one coherent queue state. History keeps the
batch's own state and host/group totals; it omits the full experiment inventory
so each batch does not repeatedly copy every other batch. Read-only status and
progress observations do not take the admission lock or rewrite authoritative
state; admission, release and provider recovery still validate and update state
under that lock. Waiters at
full capacity use these read-only hints until a reservation, pause, expiry or
free slot appears, with a bounded locked refresh for stale owners.
Live snapshots (`experiment-runner show EXPERIMENT --json`) retain `experiments`,
which gives each
active owner's PID, queued/ready/eligible-ready/running/waiting counts,
`wait_reason`, `pause_reason` and cooldown deadline. `waiting` counts shared
admission entries; additional ready threads wait locally.

To investigate empty slots, find observations where `global_running < 30`
and `global_eligible_ready > 0`, then measure the interval until admission or
a change in eligibility. Startup observations can meet this condition briefly;
it is not itself proof of a stall. Sort observations by `monotonic_seconds`
within one `boot_id`: concurrent threads can append captured observations in
a different order, and monotonic times cannot be compared across host reboots.
Progress samples are at most once per second; transition observations provide
finer timing without a new polling service. History writes occur outside the
host-wide admission lock. It contains counts and opaque owner IDs, not prompts,
answers, credentials or account/transport scope IDs. Resume appends to the
history and preserves existing observations and model receipts. Already-loaded
coordinators begin retaining this history after their next individual restart;
past readiness cannot be reconstructed from older logs.

## Saved-run compatibility

Preparation freezes inputs, model settings and request identity, and records
runner code hashes as provenance. The native worker records `request_code_sha256`
(request-producing code and pinned dependencies) separately from its full
`runner_sha256` (including scheduling and backoff). Scheduling-only changes
can resume in the same experiment directory and preserve exact answer reuse.
Worker and workflow transitions are recorded in append-only audit ledgers;
original manifests, inputs and receipts are never rewritten. The release
identifier is available with `--version`.

The checked-in compatibility registry maps specifically reviewed worker/request
fingerprints to compatible request identities. Unknown fingerprints retain strict
checks. Raw fingerprints and receipt prefixes remain audited.

Only add a historical mapping after verifying identical native serialization
and receipt/budget behavior, including the affected SSE, WebSocket, cached-prefix
and System One request paths. Preserve existing mappings when adding new ones.

## Reading and writing large evidence files

History discovery keeps request keys and original receipt references, then
releases each historical manifest and receipt body. Source verification lazily
loads matching evidence through a cache holding up to 16 MiB of smaller source
files, the executing batch's manifest, and one larger source file. A different
large source evicts the previous one before reading and decoding its replacement;
small reads do not. This avoids reparsing
one large historical manifest for every reused answer without retaining every
historical manifest. Evicted JSON is read and parsed again when needed.
Adapters may use `read(path, project=selector)` to retain a pure selected JSON
view under a separate 16 MiB bound. Ordinary source reads cannot evict those
views. Projection identity and the complete file stamp identify each entry;
full reads and different projections stay separate. The reader parses the full
source and retains its original SHA-256 before selecting fields. Oversized
views are returned without retention. No validation verdict is cached.
Digest checks keep only file identity, size, timestamps and SHA-256;
unchanged files need no repeated parsing or hashing, even above the JSON cache
limit. Changed files are hashed in 1 MiB chunks and checked for changes during
that read. Verified JSON reads populate the same digest metadata. This is a
cache bound, not a resident-memory guarantee; keep the
workload cgroup limit in place.

Canonical JSON digests stream the same sorted UTF-8 bytes, including the final
newline, rather than allocating a second complete JSON document. Existing
request, plan and receipt identities remain byte-for-byte identical.
Loading a frozen batch also shares exactly equal rendered-prompt strings between
its checked plan, config and manifest. Each file retains its own validation and
hash; the in-memory texts no longer need three separate copies.

Receipt reports parse JSONL as a stream. Generation accounting retains billing
fields and original file/line/hash references, and streams source ledgers through
the normal provenance and receipt-hash checks. Reused generations, their earlier
attempts and inherited prefix warmups retain their existing classification and
deduplication; missing usage and cost remain unknown.

Intentional repeats, including prefix warmups, skip history discovery because
they always send a fresh request. They still preserve request locks and original
receipt references. Warmup preparation reuses the frozen prefix eligibility and
loads the parent settings without building a parent report that it cannot use.

Related offline evidence checks can use `experiment.report_scope()` to share
parsed files, frozen plan validation and receipt indexes. File identity, size,
mtime and ctime are checked before reuse; changed files are read and validated
again. Receipt, request, schema and review checks still run. The scope retains
no verdicts across operations. `isolated=True` starts a fresh scope for one batch
so a long hierarchy advancement does not retain the whole experiment history
in memory. `result_records(path)` supplies the receipt-digest index used by
project failed-call and reused-producer review checks.

Receipt history discovery, history reference lookup and resume ledger loading
read one JSONL line at a time. Remembering a completed call keeps the first
exact matching line and its original hash. Worker and wrapper upgrade verification hash
saved receipt prefixes in chunks of at most 1 MiB. Completed successful prompt
receipts are released after scheduler accounting; responses needed for the
batch output remain, and saved receipts are validated again on resume.

Receipt remembering indexes first line numbers and byte offsets by hash, then
rereads the chosen line to compare its exact bytes. Confirmed local appends
advance this compact index; an unexpected file stamp, replacement, truncation
or incomplete tail rebuilds it. Receipt bodies are not cached, and ordinary
completions do not repeatedly scan the growing ledger.

Atomic JSON documents use incremental UTF-8 encoding with the same property
order, compact separators and final newline. Sorted native outputs keep sorted
keys. Serialization retains at most an encoded scalar rather than another copy
of the entire document; file flush, fsync and atomic replacement still precede
publication. Parsed inputs and report evidence still require memory, so these
changes do not replace a workload memory limit.

## Recorded performance checks

These are measurements from specific Oracle workloads, retained as development
evidence. They are not general resource requirements or guaranteed savings.

### Large-batch startup

The October 13 model-free full-grading check loaded all 128 prepared batches
and their shared history concurrently under a 32 GiB cgroup limit. It completed in
14.84 seconds, peaked at 21.94 GiB of cgroup memory and held 14.16 GiB after all
loads, with zero OOM events and zero swap. This verifies input/history startup
at full batch fanout; model workers and subsequent collection still require
runtime measurement. All ready batches continue to use the shared request
queue without an outer batch concurrency cap.

### Prefix-cache comparison

The completed 1,032-attempt, eight-block comparison measured **13.0659% lower
total API-equivalent cost**, including eight warmups, with no failures or
retries. Every block was cheaper; the exact balanced-randomization p-value was
1/70. Cache reads occurred in 128 of 512 candidate comparisons. Three of 512
matched pairs differed on one J-tubes/Food preparation case, with variation in
both formats and unchanged supplied meanings. This is bounded workload evidence,
not semantic certification or a guaranteed hit rate. The configuration uses no
public-API explicit cache options or breakpoint fields. Frozen study evidence is
preserved in the Oracle worktree
`oracle-mvp-canonical-cache-controls-v1/target/canonical-cache-sse-controlled-v4/`.
