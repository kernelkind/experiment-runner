# Experiment runner

This repository owns experiment preparation, execution, receipt verification,
request reuse, budgets, provider retries and transport. Project Git worktrees
share this implementation. Project prompts, specs, adapters and generated
experiments remain in their project repositories.

Only `run` sends model requests, and it requires the caller's existing inference
and spending authorization. Moving or installing this tool does not authorize
inference. Tests use local fixtures.

Prepared prompt experiments also support `provider: local` with `transport:
http`, an explicitly selected model worker, and an optional positive
`max_output_tokens`. Local requests reject `auth_file`; HTTP transport is
reserved for this provider. The project supplies the worker and model server.
The shared runner retains the same input, schema, worker-identity, receipt,
reuse and retry checks. Saved case reports expose the worker's
`response_metadata`, `usage` and `stop_reason` without inventing missing values.

## Contain a large workload's memory

On a host with a systemd user manager and delegated memory controller, launch
the whole coordinating command as one transient user service. Its Python and
Node children inherit the same cgroup limit. The following example bounds the
full distillation grading workload to 32 GiB with no swap, while every ready
native batch remains free to enter the existing 30-request queue:

```bash
systemd-run --user --wait --pipe --collect \
  --unit=distillation-full-1800 \
  --working-directory=/home/dev/src/notedeck_worktrees/algos/oracle-mvp-grading \
  --property=MemoryMax=32G --property=MemorySwapMax=0 \
  --property=OOMPolicy=kill --property=KillMode=control-group \
  --property=Restart=no --expand-environment=no --setenv=PATH \
  /etc/profiles/per-user/dev/bin/uv run --no-project --with tiktoken==0.14.0 \
  /home/dev/src/notedeck_worktrees/algos/experiment-runner/bin/experiment-runner \
  run-distillation-automatic-grading \
  target/distillation-review-v1/grading/full-1800/automatic \
  >> target/distillation-review-v1/grading/full-1800/run-automatic.log 2>&1
```

Run this from that task worktree so the output path also resolves there. Select
an explicit workload memory allowance for the host; 32 GiB is the approved
allowance for this grading run on Anvil. Use the installed absolute `uv` path
on another host. The service inherits the user manager's environment; pass any
required task-specific environment explicitly with `--setenv`. This changes no
host-wide memory settings and adds no concurrency limit.

If the cgroup exhausts its memory allowance, `OOMPolicy=kill` stops the whole
workload, including its children. `systemd-run --wait` returns failure and
reports `oom-kill`; `Restart=no` prevents an automatic OOM restart loop. Treat
that as a failed run. Inspect and fix the cause, then explicitly resume the
same saved automatic directory with this command. Existing model receipts and
attempt budgets stay in that directory. Supply `--runner-upgrade-reason` only
when the native source-change check requires the reviewed upgrade reason.
Stopping the group releases only its own processes; unrelated workloads remain
outside it.

History discovery keeps request keys and original receipt references, then
releases each historical manifest and receipt body. Source verification lazily
loads matching evidence through a cache limited to 16 MiB of source-file bytes,
plus the executing batch's manifest. Evicted JSON is read and parsed again when
needed. Digest checks keep only file identity, size, timestamps and SHA-256;
unchanged files need no repeated parsing or hashing, even above the JSON cache
limit. Changed files are hashed in 1 MiB chunks and checked for changes during
that read. Verified JSON reads populate the same digest metadata. This is a
cache bound, not a resident-memory guarantee; keep the
workload cgroup limit in place.

Intentional repeats, including prefix warmups, skip history discovery because
they always send a fresh request. They still preserve request locks and original
receipt references. Warmup preparation reuses the frozen prefix eligibility and
loads the parent settings without building a parent report that it cannot use.

The October 13 model-free full-grading check loaded all 128 prepared batches
and their shared history concurrently under that 32 GiB limit. It completed in
14.84 seconds, peaked at 21.94 GiB of cgroup memory and held 14.16 GiB after all
loads, with zero OOM events and zero swap. This verifies input/history startup
at full batch fanout; model workers and subsequent collection still require
runtime measurement. All ready batches continue to use the shared request
queue without an outer batch concurrency cap.

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
`experiment-runner prepare --help` includes the caching option, an automatic
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

A project extension may implement `prompt_sources(prompt)`. Preparation calls
it for a spec's prompt and for an explicit `--prompt` replacement, before
publishing the experiment. The callback receives `id`, `text` and `source_path`;
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

## Internal request queue

**Launch all ready, authorized model batches immediately. Start their `run`
processes concurrently, before waiting for completion.** The shared runner
limits active model requests, not the number of `run` processes. Do not add
outer batch caps, semaphores, fixed-size thread pools, or execution waves to
control provider concurrency. Limiting "batch coordinators" still hides ready
model work from the runner. For example, with ten independent one-request
batches, starting only four makes the other six unavailable to the queue even
when request slots are free.

`run` blocks until its own batch finishes. `prepare` freezes a batch; it does
not submit it to a background execution service. The shared queue can only
admit requests from `run` processes that have actually started. Preserve
genuine input dependencies: if B needs A's output, validate A before starting B;
independent C should start alongside A. Start newly ready batches without
waiting for unrelated batches. Keep any separate CPU/GPU limit for local
encoding from withholding ready provider batches.

From a project task worktree, with two already-prepared, authorized independent
experiments, start both before waiting:

```bash
../experiment-runner/bin/experiment-runner run target/a-v1 \
  > target/a-v1/run.log 2>&1 &
batch_a_pid=$!
../experiment-runner/bin/experiment-runner run target/b-v1 \
  > target/b-v1/run.log 2>&1 &
batch_b_pid=$!
batch_a_status=0
wait "$batch_a_pid" || batch_a_status=$?
batch_b_status=0
wait "$batch_b_pid" || batch_b_status=$?
```

Check both exit statuses and saved reports before starting dependent work.
Supply the attempt/spending limits required by your authorization to each
`run`; the queue does not grant additional inference or spending permission.

`run` shares one private host-local queue across experiments started by this
user. At most **30 requests** are admitted across all accounts and transports;
warmups and retries use those same slots. There is no concurrency CLI option.
Remove `runner.concurrency` from new specs. Historical plans/configs remain
readable and keep that field unchanged; it does not control dispatch.

When an overall experiment produces multiple batches, give all of them the
same **scheduling group**. Slots are shared between groups first, then between
their batches. A hundred ready distillation batches count as one contender
against a hierarchy experiment. With sufficient ready work, two groups balance
near 15 requests each, three near 10. A newcomer gets freed slots until it
catches up; active requests finish normally. Groups with insufficient eligible
work lend their unused capacity to the others. Account recovery and endpoint
limits still apply.

Set membership once during preparation:

```bash
bin/experiment-runner prepare target/batch-a --spec comparisons.json \
  --scheduling-group distillation
```

Every child batch from that overall experiment must receive the same ID.
Specs may instead provide top-level `"scheduling_group": "distillation"`.
`prepare --from` inherits membership, including when selecting cases or creating
a warmup. An ordinary ungrouped batch has its own allocation. IDs are 1–64
letters, digits, dots, underscores or hyphens, beginning with a letter or digit;
they identify the effort, not the prompt, model, cache key, batch or agent.
Independent workflows belonging to the same effort must share the same ID.

An older ungrouped prepared batch can join a group on its next run:

```bash
bin/experiment-runner run target/older-batch --scheduling-group distillation
```

Keep that run's existing authorization, budgets and limits. The runner saves
membership in `run/scheduling-group.json`; subsequent resumes retain it without
the flag. Conflicting prepared or saved membership is rejected before inference.
Grouping changes no model request, saved receipt, cache key or exact-reuse
identity. It cannot reassign a running coordinator: loaded coordinators retain
their old scheduling policy until individually drained and resumed. Existing
ungrouped coordinators remain readable; they do not implement group fairness.

`show --json` reports `scheduling.scheduling_group` and `scheduling.groups`, with
each group's batches, running, queued, ready and eligible-ready counts. Progress
includes `group` and `group_running`; timestamped scheduling history retains
the same coherent group counts. Fairness is per explicitly identified effort,
not per agent or token budget.

Waiting cases stay in the runner process; model workers start only after
admission. Native workers advertise `experiment_scopes: true` in `--identity`
and implement `scopes`: read the selected account's local configuration, query
the local credential service and return only hashed account/transport scopes,
without inference. The runner discovers scopes once per batch and the worker
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

Existing account/transport cooldowns and gradual recovery can reduce the
effective capacity below 30 when there is explicit rate-limit/overload evidence,
a valid `Retry-After`, or a verified connection failure. HTTP/2 stream
`INTERNAL_ERROR` and generic server errors without those signals delay only
the failed request's retry. Fresh cases keep using free slots, and active
requests finish normally. Unknown transport errors do not establish shared
pressure or automatic retry eligibility.

Shared pressure keeps the bounded 2/4/8/.../60-second backoff and gradual
recovery: start with one new request, then double the admission limit after
that many successful completions. Failures from requests already in flight
share one backoff generation; a late failure does not restart recovery or
extend the wait unless it carries an explicit `Retry-After`. A new failure
during recovery can increase backoff. Resume does not recreate a new outage
from an old receipt. **Every verified temporary provider failure retries
automatically until success or an authorized limit, pause or cancellation.**
Per-case delays are 2/4/8/16/32/60/60... seconds, using the saved lifetime
attempt counter. There is no two-attempt cutoff, including on resume. Warmups
use the same policy. Longer valid `Retry-After` or `retry-after-ms` deadlines
take precedence; if both are present, honor the longer one. Malformed headers
fall back to exponential delay. The native worker still makes exactly one
transport attempt per invocation; retries have separate immutable receipts.

The production classifier covers these recorded errors:

| Evidence | Retry behavior |
| --- | --- |
| HTTP 408, 409, 425, 429 or 5xx; numeric equivalents inside an error stream | Retry, unless a structured permanent error or explicit no-retry header takes precedence. Symbolic body codes do not hide the actual HTTP status. |
| `server_error`, `server_is_overloaded`, rate-limit/temporary usage-limit codes, `websocket_connection_limit_reached`, service-unavailable/overloaded types | Retry for SSE and WebSocket error responses. |
| OpenRouter `provider_overloaded`, `provider_unavailable`, `server`, `timeout` | Retry; canonical request, output-limit and refusal types take precedence over generic numeric codes. |
| Verified active proxy interruption; structured socket/connect/reset/timeout/temporary-DNS cause codes | Retry the failed request. Arbitrary exception prose is insufficient. Local aborts and canceled/deadline-expired request contexts stop. |
| WebSocket closes before a terminal response: 1000, 1001, 1006, 1011–1014 | Retry after a completed upgrade and recorded body interruption. Protocol, unsupported-data, policy and size failures are not retryable. |
| OpenRouter 402 with `limit_source=openrouter_in_flight_budget` and a valid retry deadline | Retry the documented temporary in-flight spending wait. Ordinary exhausted credit remains fatal. |
| Explicit `x-should-retry` | Honor true/false without overriding authentication, exhausted credit, invalid requests, completed output or local cancellation. |

Authentication/access failures, exhausted paid credit, malformed requests,
context/output limits, local crashes/timeouts, invalid outputs, source changes,
certificate failures and unknown causes do not retry. Incomplete output alone
does not establish a temporary provider failure. Structured HTTP rejection
details are retained before the SDK reduces them to friendly text; raw rejection
bodies are not added to receipts. Classification also controls shared admission,
so a permanent error cannot impose overload recovery on unrelated work.

HTTP status/header behavior follows the [official OpenAI SDK](https://github.com/openai/openai-python/blob/main/src/openai/_base_client.py);
OpenRouter's typed errors and temporary 402 follow its [error contract](https://openrouter.ai/docs/api/reference/errors-and-debugging).
HTTP 425 follows [RFC 8470](https://www.rfc-editor.org/rfc/rfc8470#section-5.2).
Proxy HTTP/2 evidence is interpreted using [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html#section-7);
WebSocket meanings follow the [IANA registry](https://www.iana.org/assignments/websocket).
These define retry classification, not a promise that a particular retry succeeds
or that an interrupted generation was never processed. Every attempt counts
toward the existing attempt/spending allowance; missing usage remains unknown.

No retry flag is required. `--keep-retrying-provider-errors` is retained as a
compatibility option with the same automatic behavior. Successful answers are
never redrawn. Observe successful completions/minute alongside failure counts,
retry usage/cost and admission history when assessing throughput.

Rejected WebSocket upgrades preserve their actual HTTP status and retry headers
through a connection-specific dispatcher observer. A 503/429 handshake can
therefore retry; 403 stays fatal. No SSE fallback or extra handshake probe is
sent, and concurrent connections do not share response metadata.

Queued work consumes no attempt reservation;
once admitted, existing conservative accounting remains in force. Failed or
interrupted reservations are never refunded. Exact-answer reuse consumes no
slot, warmup or new attempt. A prefix warmup competes within its experiment's
share, finishes before comparisons, and counts against their attempt budget.
Scheduling adds no prompt, schema, cache-key or request-body changes.

`run` prints live progress on stderr. For machine-readable status, use:

```bash
experiment-runner show target/example-v1 --json
```

The `scheduling` object includes queued/ready/running/valid/failed/reused counts,
`global_running` out of `global_limit: 30`, per-admission case status and a
`wait_reason`: `capacity`, `fair_share`, `account_backoff`, `transport_backoff`,
`provider_backoff` (a batch's shared-pressure delay), `recovery_limit`,
`prefix_warmup` or `ready`. `queue_wait_seconds` is the longest
observed/current admission wait; it excludes worker execution and preparation
before entering admission. `running_elapsed_seconds` is the oldest current
admission's elapsed time, including worker startup. `cooldown_until_utc` records
a known cooldown deadline, not a predicted provider completion time.

`queued` includes all unfinished cases that are not admitted, including cases
waiting for a prefix warmup, their own retry deadline, or still preparing.
`ready` counts requests that have reached admission; it includes requests held
by capacity or backoff.
`eligible_ready` excludes requests held by account/transport/batch backoff,
recovery limits, occupied endpoint capacity or a requested pause. It includes
global capacity and fairness waiting. `global_ready` and
`global_eligible_ready` sum those counts across
experiments. A ready warmup counts as one request; its waiting comparisons do
not become ready until warming finishes.

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

The queue uses the existing private provider-backoff state under
`$XDG_CACHE_HOME/oracle-mvp/provider-backoff` (normally `~/.cache/...`). Process
identity and request deadlines release stale admissions after cancellation or
exit. A live orphaned model worker retains its slot even after its runner exits.
A resumed run validates saved receipts and budgets normally. Live state
is operational evidence, separate from immutable model receipts. The limit
covers this user's cooperating runner executions on this host; it is not the
provider's concurrency contract or a reservation guaranteeing chat capacity.

## Versions and saved experiments

Preparation freezes inputs, model settings and request identity, and records
runner code hashes as provenance. Version 1.6.0 separates the native worker's
`request_code_sha256` (request-producing code and pinned dependencies) from its
full `runner_sha256` (including scheduling and backoff). Scheduling-only changes
can resume in the same experiment directory and preserve exact answer reuse.
Worker and workflow transitions are recorded in append-only audit ledgers;
original manifests, inputs and receipts are never rewritten. The release
identifier is available with `--version`.

Normal `run` automatically audits compatible runner updates. Changes to project
adapters or evaluators still require inspection and `--runner-upgrade-reason`.
Changed request-producing code or transport requires a new experiment. An
explicit reason cannot override incompatible model requests. Unknown historical
or custom workers without a request-code fingerprint retain strict worker-hash
checks. The checked-in compatibility registry identifies two verified previous
native workers and one verified error-observation revision; it does not declare
every historical runner compatible. Its exact request fingerprint maps to the
unchanged serialization family; raw fingerprints and receipt prefixes remain
audited. SSE, WebSocket, cached-prefix and System One serialization were checked
against the preceding release before registering this update.
Only add a historical mapping after verifying identical native serialization
and receipt/budget behavior; preserve existing mappings when adding new ones.

For a controlled handover, pause one experiment, wait until its status is
`paused`, then resume it with the same budget and execution limits:

```bash
bin/experiment-runner pause target/trial-v1
bin/experiment-runner show target/trial-v1 --json
bin/experiment-runner run target/trial-v1 --attempt-budget target/attempt-budget.json
```

`pause` stops admission without killing requests. Active requests finish and
save receipts; queued cases consume no attempt reservations. Other experiments
continue running. `show --json` reports `pausing` while draining and `paused`
afterward. Compatible live worker updates are audited and continue without
pausing other runs. Incompatible worker-source changes automatically request a
handover before further dispatch. This is a checkpoint, not an automatic
process restart;
invoke `run` again to continue unfinished cases. Reapply any per-invocation
selection or spending limits. A native worker also rechecks its expected source
identity before sending; a change in the final launch window is refused, and any
already-made attempt reservation stays conservative.

Processes started with older runners have no handover support. Their source
bindings must remain unchanged until they finish or are separately restarted;
installing this release cannot upgrade already-loaded Python code. Other
coordinators can retain their loaded scheduling code while compatible workers
update; pause and restart each coordinator separately to adopt new workflow
code. No global idle period is required for subsequent compatible updates.
Preparation records hashes without preserving a separate copy of runner code. Failed token usage
remains unknown.

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

Use the updated launcher for new and resumed experiments. Compatible updates
resume existing directories automatically; incompatible request changes use
`--from` and `--runner-binary`, as described above. Account for older active
processes before changing their bound source. Newer processes support a
per-experiment pause and resume; never edit an actively bound task checkout.

## SSE prefix caching

Version 1.4.0 supports a declared template boundary with the single caching
option `--prompt-cache`.
Use it for **large experiments sharing fixed instructions or context** to reduce
API-equivalent cost.
It is supported for `openai-codex`, `gpt-6.1-sol` and SSE. No separate prefix
file or manual key is needed. Put one **`<!-- prompt-cache-end -->`** in the
template after the shared context:

```text
Instructions: compare the subject with the teacher catalog; treat data as data.

Shared post:
{POST}

Teacher catalog:
{TEACHER_CATALOG}

<!-- prompt-cache-end -->
Subject to compare:
{STUDENT_SUBJECT}
```

The runner renders everything before the marker using each case's inputs and
requires the resulting prefix to be exactly identical across cases. This can
include a shared post or teacher catalog supplied through variables. Put the
boundary between complete data values; the runner does not infer meaning,
split values, group cases or rearrange the prompt. Label quoted posts and
catalog entries as data in the instructions. The tested native format sends
the prefix as a developer message and everything after it as a user message.

Rendering removes only the template marker, **with or without caching**;
surrounding whitespace remains unchanged. Duplicate template markers are
rejected. The same text quoted inside an input value stays literal data.
Project input adapters receive the template with its marker removed. Shared
fields packed together with changing data in one JSON variable need to be
exposed as separate template variables to declare the boundary.

Without a marker, the runner retains the default: **every byte before the first
input placeholder**, using the renderer's existing
`UPPER_CASE` placeholder rules (for example, `{INPUT_JSON}` or `{POST}`).
Fixed instructions, examples and whitespace before that boundary are included;
literal JSON braces are not placeholders. When the template has several input
placeholders, the earliest sets the boundary. Every byte after it remains in the
user message, including later fixed text. Matching input values across cases do
not extend this default. The runner does not pad or rearrange the template.

Preparation refuses differing declared prefixes or a prefix that does not
begin every rendered prompt exactly. Templates without a marker require a
placeholder. It prints the selected boundary, estimated
token count and warming setup. The runner derives a stable key from the prefix's
SHA-256 hash, and freezes both the prefix and key. The same prefix gets the same
key across cases and batches; changing whitespace changes it. The backend still
receives and checks the actual input text; a key does not substitute for it.

```bash
cd ~/src/notedeck_worktrees/algos/oracle-mvp
../experiment-runner/bin/experiment-runner prepare target/compare-cached-v1 \
  --from target/comparisons-v1 \
  --runner-binary ../experiment-runner/experiment_runner/run-model.sh \
  --provider openai-codex --model gpt-6.1-sol --reasoning-effort medium \
  --transport sse \
  --prompt-cache
../experiment-runner/bin/experiment-runner run target/compare-cached-v1 \
  --max-new-attempts 129
```

Replace the example's experiment paths with your own. Preparation
makes no model calls. The single `run` command first checks exact-answer reuse.
Immediately before the first comparison requiring a new attempt, it sends one
prefix warmup and waits for completion, then runs the comparisons.
This uses the tested developer-prefix/user-data format and native subscription
key/routing headers. Each request remains independent; no previous answer is
included.

`run EXPERIMENT --only CASE_ID` dispatches selected frozen cases; repeat `--only`
for several cases. The saved and returned report still contains every case.
Unselected failed or unrun cases do not make the selected execution fail. The
programmatic equivalent is `run(..., only=[...])`; an empty list dispatches
nothing. Selection preserves prompts, settings, receipt validation, retries,
answer reuse and attempt budgets.

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

Preparation requires a prefix with at least **1,024 locally estimated
`o200k_base` tokens** when prefix caching is requested. A shorter prefix is
rejected with its estimated count and minimum before creating the experiment or
spending calls. For a new uncached spec, omit the caching option and
`runner.prompt_cache`. Automatic warming also requires
allowance for warmup plus a comparison. The count covers
only the supplied text, excludes message/schema overhead and is not an exact
subscription tokenizer contract. The documented public API threshold is 1,024
visible tokens ([OpenAI caching guide](https://developers.openai.com/api/docs/guides/prompt-caching));
backend eligibility and cache hits remain unguaranteed. All-reused or
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

For specs, set `runner.prompt_cache` to `true` (a JSON boolean). Later
`prepare --from` batches inherit template caching. Changing the template with
`--prompt` recomputes the prefix, count and key after the new prompt is rendered.
Without a prompt revision, `--from` preserves the frozen prefix, key and
eligibility, including with `--only` or `--repeat`; selecting fewer cases never
extends the prefix into their changing data. The prefix and boundary are frozen
into the new plan; old receipts stay unchanged.
New specs without caching preserve the previous one-user-message format.

Old manual-prefix/key/warmup experiments retain their frozen behavior. The legacy
`--prompt-cache-prefix FILE`, `--prompt-cache-key KEY` and `--cache-warmup`
options remain accepted but hidden from normal help. Supply `--prompt-cache`
to opt an older experiment into template selection. It cannot be combined with
manual cache overrides or historical import. Select the shared worker explicitly
when migrating from an incompatible historical worker. Compatible runner updates
preserve the experiment and its fixed request identity.

Adding caching changes the request's message format and identity, so uncached
answers are not reused as cached answers, even if their combined text is the
same. Identical cached requests reuse saved answers normally. `prepare --repeat`
requests independent fresh trials. WebSocket cache options are refused before
dispatch.

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

## API-equivalent cost statistics

Version 1.2.1 adds `usage.cost_statistics` to native `report.json`,
`show --json` and `--generation-costs` reports. Readable `run` and batch `show`
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
above, without allocating it between cases.

## Project extensions

An `experiment-runner.json` file declares `schema_version: 1`, an `inputs`
module and an `extension` module. Modules must belong to the selected project.
Input modules implement preparation, rendering, validation and review evidence;
extensions register project commands, collection and views, and declare their
code files. These files are hashed alongside the executor in each prepared plan.
The selected validator is resolved before worker threads start.

### Mechanical results

Version 1.3.1 accepts an optional adapter-provided field on each prepared case:

```json
{
  "mechanical": {
    "output": "{\"valid\":true,\"reason\":\"Exact trusted wording.\"}",
    "evidence": {"rule": "exact_teacher_subject", "teacher_numbers": ["1.1"]}
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
