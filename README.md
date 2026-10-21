# Experiment Runner

Experiment Runner is a command-line tool for testing language-model prompts
against datasets.

It preserves each experiment's prompt, inputs, model settings and answers so you
can inspect the results and compare changes between runs. For example, you can
test a revised classification prompt on the same examples and review its answers
alongside the saved baseline.

Experiments are defined in local files. The runner handles request execution,
retries, resuming interrupted work, reuse of saved answers, and usage accounting.

## An example

Suppose you are developing a prompt that labels the topic of a text. You run it
on a set of examples, then revise it to ask for more specific labels. The runner
lets you compare the two versions for each input:

| Input | Topic-v1 | Topic-v2 |
| --- | --- | --- |
| I planted tomatoes and watered the seedlings. | Gardening | Growing tomatoes |
| The update fixes a crash when opening saved files. | Software | Software bug fixes |

These answers are **illustrative**. The tutorial below uses these inputs and
prompt versions; actual model answers will vary. The runner records which
answers changed. You decide whether the changes are improvements, and can save
your reviews with the experiment. The original answers remain available without
rerunning the baseline.

## Requirements and installation

Use **Linux**, **Git**, **Python 3.11+**, **Node.js 24+** and **npm**. The request queue uses
Linux process information from `/proc`.

Choose how to run model requests:

| Route | Requirements |
| --- | --- |
| `openai-codex` | An account with access to the selected subscription model; browser or device-code login. |
| `openrouter` | An OpenRouter account/API key and a model supported by the bundled worker. |
| `local` | Your own compatible HTTP model worker and server. The runner does not install or host models. |

The tutorial uses `openai-codex` with `gpt-6.1-sol`.
[Provider and authentication instructions](docs/running-experiments.md#providers-and-authentication)
cover the other routes and the existing Anvil account-service integration.

Source publication is planned for
[kernelkind/experiment-runner](https://github.com/kernelkind/experiment-runner).
After publication, run the following from the directory where you want the
checkout. If you already have `experiment-runner/`, skip the clone line:

```bash
git clone https://github.com/kernelkind/experiment-runner.git
cd experiment-runner
export EXPERIMENT_RUNNER_ROOT="$PWD"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
npm --prefix experiment_runner/frontier_runner ci \
  --ignore-scripts --no-audit --no-fund
experiment-runner --help
```

Continue in the same shell with the virtual environment active. The installed
`experiment-runner` command works from your experiment's directory; the source
checkout remains available through `EXPERIMENT_RUNNER_ROOT`.

## Run your first experiment

### 1. Define the prompt and inputs

Use a fresh `~/experiment-runner-example` directory. Create a **Topic-v1** prompt,
two named cases, and a spec selecting their files and model settings:

```bash
mkdir -p ~/experiment-runner-example
cd ~/experiment-runner-example
cat > prompt-v1.txt <<'PROMPT'
Identify the main topic of this text. Return only a short topic label.

{TEXT}
PROMPT
cat > cases.json <<'JSON'
{
  "cases": [
    {
      "id": "gardening",
      "inputs": {"TEXT": "I planted tomatoes and watered the seedlings."}
    },
    {
      "id": "software",
      "inputs": {"TEXT": "The update fixes a crash when opening saved files."}
    }
  ]
}
JSON
cat > spec.json <<'JSON'
{
  "schema_version": 1,
  "prompt": {"id": "Topic-v1", "path": "prompt-v1.txt"},
  "dataset": {"adapter": "generic", "path": "cases.json"},
  "runner": {
    "provider": "openai-codex",
    "model": "gpt-6.1-sol",
    "reasoning_effort": "medium",
    "transport": "sse"
  }
}
JSON
```

For each case, the runner replaces `{TEXT}` with that case's input. Case IDs must
be unique; variable names are `UPPER_CASE` and values are strings. File paths in
the spec are relative to the spec file. The
[input-format reference](docs/running-experiments.md#prompts-cases-and-specs)
covers additional variables and structured responses.

### 2. Prepare and inspect

```bash
cd ~/experiment-runner-example
experiment-runner prepare run-v1 --spec spec.json
experiment-runner show run-v1 --case gardening --prompt-only
```

Preparation saves the exact prompts and settings in a new directory and records
source/code hashes. It makes no model calls. The two cases are initially `unrun`.
The second command prints the prompt with the gardening text substituted.

Keep `prompt-v1.txt` and `cases.json` unchanged: later checks use their recorded
hashes. Settings are frozen inside `run-v1`. Keep the spec for future experiments,
and write a new file for a prompt revision.

### 3. Authenticate and execute

For access through your own provider account, select `direct`. The current
default is Anvil's account service, so set this explicitly outside Anvil.
If you have not already authenticated the runner, log in:

```bash
cd "$EXPERIMENT_RUNNER_ROOT"
export EXPERIMENT_RUNNER_AUTH=direct
./experiment_runner/run-model.sh login openai-codex
```

Credentials are stored outside the repository, normally in
`~/.config/experiment-runner/auth.json`. If you are already authenticated, set
`EXPERIMENT_RUNNER_AUTH=direct` in your shell and continue.

The next command sends model requests using your account:

```bash
cd ~/experiment-runner-example
experiment-runner run run-v1
experiment-runner show run-v1 --case gardening
```

`run` prints progress, saves responses as they arrive, and waits for completion.
`show --case` displays the input, prompt, answer, settings and recorded evidence.
Use `show run-v1` for a summary or add `--json` for machine-readable output.

To resume after an interruption, run the same command against `run-v1`.
Completed successful requests are retained. Temporary provider failures retry
automatically. [Execution limits](docs/running-experiments.md#execution-limits)
explain how to bound attempts and OpenRouter spending for larger runs.

## Compare a revised prompt

Create **Topic-v2**, which asks for a more specific label, and prepare a new
experiment from the first:

```bash
cd ~/experiment-runner-example
cat > prompt-v2.txt <<'PROMPT'
Identify the main topic of this text. Return only a specific topic label
of at most three words.

{TEXT}
PROMPT
experiment-runner prepare run-v2 --from run-v1 \
  --prompt prompt-v2.txt --prompt-id Topic-v2
experiment-runner show run-v2 --case gardening --prompt-only
```

This inherits the inputs and model settings and saves the first experiment's
answers as its baseline. Preparation makes no model calls. After checking the
new prompt, execute and inspect the candidate:

```bash
cd ~/experiment-runner-example
experiment-runner run run-v2
experiment-runner show run-v2 --case gardening
```

The case view contains both the candidate answer and its `baseline`, with a
comparison such as `changed` or `unchanged`. These describe text changes, not
quality scores. A `valid` result means it passed the configured validation; the
plain-text example above has no check for correct topic labels.

You can record your assessment using `accept`, `reject` or `uncertain`. For
example, after inspecting the answer:

```bash
cd ~/experiment-runner-example
experiment-runner review run-v2 --case gardening \
  --role user --verdict uncertain \
  --comment "Check whether the label is specific enough."
```

Reviews remain separate from model responses and validation results.

## Working with saved experiments

- **Resume:** run the existing directory again to continue unfinished work.
- **Compare:** prepare a new directory with `--from` to retain the old answers as
  a baseline. Keep related experiments under the same parent directory so their
  default request-history search can find each other.
- **Reuse:** identical requests reuse verified saved answers. Reuse is not an
  independent model sample. For an intentional fresh trial, prepare a new
  directory with `--repeat`.
- **Keep sources:** do not edit frozen prompts, inputs, settings or native run
  files. The runner checks their recorded hashes. Retain the corresponding code
  revision as well; a hash is not a copy of the implementation.

Each directory contains the frozen experiment (`experiment.json`), model-worker
configuration (`runner-config.json`), latest report (`report.json`) and execution
records (`run/`). Standalone prompt experiments also maintain a compact local
`publication/` export of settings, inputs, answers and reviews. It contains real
case data; export does not upload anything. See
[saved results](docs/saved-results.md) for the formats and verification limits.

## Commands

Run `experiment-runner COMMAND --help` for options.

| Task | Command |
| --- | --- |
| Prepare from files | `prepare DEST --spec SPEC` |
| Prepare a prompt revision | `prepare DEST --from OLD --prompt FILE --prompt-id VERSION` |
| Execute or resume | `run EXPERIMENT` |
| Inspect a case | `show EXPERIMENT --case ID` |
| Inspect saved inputs | `show EXPERIMENT --inputs-only` |
| Record a review | `review EXPERIMENT --case ID --role user --verdict accept --comment TEXT` |
| Let active requests finish and stop new ones | `pause EXPERIMENT` |
| Create a revised prompt from exact text edits | `revise BASE DEST --edits FILE` |
| Export saved prompt results locally | `export-results EXPERIMENT` |
| Check saved prompt evidence | `verify EXPERIMENT` |

For the core prompt workflow, `run` performs inference. `reconcile-costs`
separately queries OpenRouter for missing charges without generating answers.

## Further documentation

| Guide | Contents |
| --- | --- |
| [Running experiments](docs/running-experiments.md) | Input format, authentication, execution limits, retries, parallel runs, upgrades and prefix caching. |
| [Saved results](docs/saved-results.md) | Reports, costs, compact exports, integrity checks and relocating native evidence. |
| [Project extensions](docs/extensions.md) | Custom preparation, validation and review, Python integration, and Oracle MVP boundaries. |
| [Development](docs/development.md) | Tests, source launchers, execution internals, compatibility and recorded performance checks. |

## Project extensions

The built-in `generic` adapter works without another project. Projects can add
input preparation, validation, review interfaces and commands through an
`experiment-runner.json` configuration.

Oracle MVP uses this mechanism, and the runner still contains some Oracle-specific
extraction and compatibility code. The
[Oracle integration notes](docs/extensions.md#oracle-mvp) describe exactly which
paths require it. No Oracle checkout is needed for the tutorial above.

For development, start with the [test instructions](docs/development.md#tests).
