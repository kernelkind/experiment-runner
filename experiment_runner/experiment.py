#!/usr/bin/env python3
"""Prepare, run, compare, revise and review preserved prompt experiments.

Only `run` performs inference. See this repository's README and the selected
project's workflow documentation.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextvars import copy_context
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import fcntl
import json
import math
import statistics
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
from contextvars import ContextVar
from types import SimpleNamespace

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from experiment_runner import extraction_pipeline as engine, project
from experiment_runner.project import inputs
from experiment_runner.inputs import CACHE_PREFIX_END, template_prefix, without_cache_marker
from experiment_runner.prompt_cache import prefix_policy


class ReportFiles(engine.ReceiptFiles):
    """One operation's parsed evidence; changed files invalidate cached reads."""

    def __init__(self):
        super().__init__()
        self.loads = {}
        self._ledgers = {}

    def ledger(self, path):
        path = Path(path)
        with self._lock:
            stamp = self._stamp(path)
            entry = self._ledgers.get(path)
            if entry is None or entry['stamp'] != stamp:
                raw = path.read_bytes()
                if self._stamp(path) != stamp:
                    raise engine.RunnerError(f'source changed while reading: {path}')
                lines = raw.splitlines(keepends=True)
                entry = dict(stamp=stamp, lines=lines, records=[json.loads(line) for line in lines])
                self._ledgers[path] = entry
            return entry


_REPORT_FILES = ContextVar('experiment_report_files', default=None)


@contextmanager
def report_scope(*, isolated=False):
    """Share file reads across related reports, never across operations."""
    if not isolated and _REPORT_FILES.get() is not None:
        yield _REPORT_FILES.get()
        return
    token = _REPORT_FILES.set(ReportFiles())
    try:
        yield _REPORT_FILES.get()
    finally:
        _REPORT_FILES.reset(token)


def result_records(path):
    """Index native receipt records once per unchanged ledger in this scope."""
    files = _REPORT_FILES.get() or ReportFiles()
    with files._lock:
        entry = files.ledger(path)
        if 'by_digest' not in entry:
            entry['by_digest'] = {inputs.digest(record): record for record in entry['records']}
        return entry['by_digest']


def read(path):
    files = _REPORT_FILES.get()
    if files is not None:
        try:
            return files.json(Path(path))
        except (OSError, UnicodeError, ValueError):
            # Keep the established error diagnostics on malformed/missing files.
            return engine._load_json(Path(path), str(path))
    return engine._load_json(Path(path), str(path))


def write(path, value):
    # Schema property order is part of request identity; do not sort nested keys.
    engine._atomic_write_json(Path(path), value)


def resolve(path, base):
    path = Path(path).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


@contextmanager
def locked(directory):
    with (directory / ".experiment.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("experiment is already active") from error
        yield


def load(directory):
    directory = Path(directory).resolve()
    files = _REPORT_FILES.get()
    saved = files.loads.get(directory) if files is not None else None
    if saved is not None and saved['stamps'] == {
            path: files._stamp(path) for path in saved['stamps']}:
        return saved['value']
    stamps = {}
    def observe(path):
        if files is not None:
            stamps[path] = files._stamp(path)
        return path
    envelope = read(observe(directory / "experiment.json"))
    plan = envelope["plan"]
    if inputs.digest(plan) != envelope["sha256"]:
        raise ValueError("prepared experiment changed; prepare a new experiment")
    for name, expected in plan["files"].items():
        path = observe(directory / name)
        if (files.sha256(path) if files else engine._sha256_file(path)) != expected:
            raise ValueError(f"prepared {name} changed; prepare a new experiment")
    config = engine.PromptRunnerConfig.load(observe(directory / "runner-config.json"), files=files)
    manifest = read(observe(Path(plan["run_directory"]) / "manifest.json"))
    if inputs.digest(manifest) != plan["manifest_sha256"]:
        raise ValueError("prepared run manifest changed")
    value = plan, config, manifest
    if files is not None and stamps == {path: files._stamp(path) for path in stamps}:
        files.loads[directory] = dict(stamps=stamps, value=value)
    return value


def spec_setup(path, *, preparation_context=None, include_request_preview=True):
    path = Path(path).resolve()
    spec = read(path)
    engine._expect_fields(
        spec, "experiment spec", {"schema_version", "prompt", "dataset", "runner"}
        | ({"scheduling_group"} & spec.keys())
    )
    if type(spec["schema_version"]) is not int or spec["schema_version"] != 1:
        raise ValueError("expected experiment schema_version 1")
    from . import scheduling
    group = scheduling.validate_group(spec.get('scheduling_group'))
    prompt = spec["prompt"]
    engine._expect_fields(prompt, "prompt", {"id", "path"})
    prompt_path = resolve(prompt["path"], path.parent)
    prompt = {
        "id": engine._expect_identifier(prompt["id"], "prompt.id"),
        "text": prompt_path.read_bytes().decode(),
        "source_path": str(prompt_path),
    }
    prompt_sources = project.prompt_sources(prompt)
    dataset = spec["dataset"]
    engine._expect_fields(
        dataset,
        "dataset",
        {"adapter", "path"} | ({"select", "mask_format", "combine_masks", "prominence_basis"} & dataset.keys()),
    )
    source_path = resolve(dataset["path"], path.parent)
    context = {} if preparation_context is None else {'preparation_context': preparation_context}
    cases, sources, preparation = inputs.prepare_file(
        dataset, source_path, without_cache_marker(prompt["text"]),
        lambda directory: report(directory, include_request_preview=include_request_preview),
        **context)
    runner = dict(spec["runner"])
    if 'concurrency' in runner:
        raise ValueError('runner.concurrency is no longer configurable; the internal queue shares 30 requests across experiments')
    if "output_schema_path" in runner:
        if "output_schema" in runner:
            raise ValueError("choose output_schema or output_schema_path, not both")
        schema_path = resolve(runner.pop("output_schema_path"), path.parent)
        schema_bytes = schema_path.read_bytes()
        runner["output_schema"] = json.loads(schema_bytes)
        sources[str(schema_path)] = engine._sha256_bytes(schema_bytes)
    runner["runner_binary"] = str(
        engine._resolve_executable(
            runner.get("runner_binary", str(engine.DEFAULT_RUNNER)),
            "runner_binary",
            path.parent,
        )
    )
    runner["auth_file"] = (
        str(resolve(runner["auth_file"], path.parent))
        if runner.get("auth_file")
        else None
    )
    return {
        **({'scheduling_group': group} if group is not None else {}),
        "prompt": prompt,
        "dataset": {**dataset, "path": str(source_path)},
        "cases": cases,
        "preparation": preparation,
        "runner": runner,
        "sources": {
            **sources,
            **prompt_sources,
            str(path): engine._sha256_file(path),
            str(prompt_path): engine._sha256_file(prompt_path),
        },
    }


def uses_mechanical(manifest, case):
    return "mechanical" in case and not manifest.get("request_reuse", {}).get("intentional_repeat", False)


def mechanical_result(dataset, case, schema):
    """Validate an adapter's frozen decision; never manufacture a model receipt."""
    decision = case["mechanical"]
    engine._expect_fields(decision, "case.mechanical", {"output", "evidence"})
    raw, evidence = decision["output"], decision["evidence"]
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("case.mechanical.output must be a nonempty JSON string")
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError("case.mechanical.evidence must be a nonempty JSON object")
    json.dumps(evidence, allow_nan=False)

    def invalid_constant(value):
        raise ValueError(f"invalid JSON constant in mechanical output: {value}")
    json.loads(raw, object_pairs_hook=engine._position_decoder().unique_object,
               parse_constant=invalid_constant)
    result = inputs.validate(dataset, case, raw, schema)
    if result.get("status") not in {"valid", "empty"}:
        raise ValueError("mechanical output failed adapter validation")
    return {**result, "origin": "mechanical", "mechanical_evidence": evidence,
            "mechanical_sha256": inputs.digest(decision), "attempts": 0, "reused": False}


def request_preview(plan, manifest, cases, *, include_reusable_case_ids=False):
    pending = [case for case in cases if case.get("status", "unrun") == "unrun"
               and not uses_mechanical(manifest, case)]
    reusable = 0
    reusable_case_ids = []
    if pending and not manifest["request_reuse"]["intentional_repeat"]:
        history = engine.RequestHistory(
            Path(manifest["request_reuse"]["history_root"]),
            exclude_run=Path(plan["run_directory"]),
        )
        for case in pending:
            for _, source in history.matches(
                engine._prompt_request_key(manifest, case["prompt"])
            ):
                try:
                    inputs.validate(
                        plan["dataset"],
                        case,
                        source["final_output_raw"],
                        plan["runner"].get("output_schema"),
                    )
                except (ValueError, engine.RunnerError):
                    continue
                reusable += 1
                if include_reusable_case_ids:
                    reusable_case_ids.append(case['id'])
                break
    unrun = len(pending)
    result = {
        "unrun": unrun,
        "reusable": reusable,
        "new": unrun - reusable,
        "note": "lookup at inspection time; run rechecks verified receipts",
    }
    if include_reusable_case_ids:
        result['reusable_case_ids'] = reusable_case_ids
    return result


def prepare(args, *, preparation_context=None, include_request_preview=True):
    from . import scheduling
    destination = args.destination.expanduser().resolve()
    if destination.exists():
        raise ValueError("experiment destination already exists")
    warmup = getattr(args, "cache_warmup", False)
    inherited_history = None
    if args.from_experiment:
        old, _, old_manifest = load(args.from_experiment)
        inherited_history = old_manifest.get("request_reuse", {}).get("history_root")
        setup = {k: old[k] for k in ("prompt", "dataset", "cases", "runner", "sources")}
        group = old.get('scheduling_group') or scheduling.bound_group(Path(old['run_directory']))
        if group is not None:
            setup['scheduling_group'] = group
        setup['preparation'] = old.get('preparation')
        if 'cache_warmup' in old:
            setup['cache_warmup'] = old['cache_warmup']
        baseline = (None if warmup else report(Path(args.from_experiment).resolve(),
                                               include_request_preview=include_request_preview))
    else:
        setup = spec_setup(args.spec, preparation_context=preparation_context,
                           include_request_preview=include_request_preview)
        baseline = report(args.baseline.resolve(), include_request_preview=include_request_preview) if args.baseline else None
    group = scheduling.validate_group(getattr(args, 'scheduling_group', None))
    if group is not None:
        if setup.get('scheduling_group') not in (None, group):
            raise ValueError('scheduling group differs from the supplied experiment')
        setup['scheduling_group'] = group
    if args.only:
        requested = set(args.only)
        unknown = requested - {c["id"] for c in setup["cases"]}
        if unknown:
            raise ValueError(f"unknown --only case IDs: {', '.join(sorted(unknown))}")
        setup["cases"] = [c for c in setup["cases"] if c["id"] in requested]
        if baseline is not None:
            baseline = {**baseline, "cases": [c for c in baseline["cases"] if c["id"] in requested]}
    # Job IDs from historical batches are not the candidate's stable case IDs.
    setup["cases"] = [
        {k: v for k, v in c.items() if k != "job_id"} for c in setup["cases"]
    ]
    overrides = {
        k: getattr(args, k)
        for k in (
            "model",
            "reasoning_effort",
            "provider",
            "max_output_tokens",
            "transport",
            "runner_binary",
            "prompt_cache_key",
        )
        if getattr(args, k) is not None
    }
    setup["runner"] = {**setup["runner"], **overrides}
    cache_requested = getattr(args, 'prompt_cache', False)
    if 'prompt_cache' in setup['runner']:
        specified_cache = setup['runner'].pop('prompt_cache')
        if type(specified_cache) is not bool:
            raise ValueError('runner.prompt_cache must be a boolean')
        if specified_cache and any(name in setup['runner'] for name in ('prompt_cache_prefix', 'prompt_cache_key')):
            raise ValueError('runner.prompt_cache cannot be combined with manual cache settings')
        cache_requested = cache_requested or specified_cache
    prefix_path = getattr(args, "prompt_cache_prefix", None)
    if cache_requested and (args.import_config or warmup or prefix_path is not None
                            or getattr(args, 'prompt_cache_key', None) is not None):
        raise ValueError('--prompt-cache cannot be combined with historical import or manual cache settings')
    if args.runner_binary is not None:
        setup["runner"]["runner_binary"] = str(engine._resolve_executable(
            str(args.runner_binary), "runner_binary", Path.cwd()))
    if args.prompt:
        if not args.prompt_id:
            raise ValueError("--prompt requires --prompt-id")
        path = args.prompt.expanduser().resolve()
        setup["prompt"] = {
            "id": args.prompt_id,
            "text": path.read_bytes().decode(),
            "source_path": str(path),
        }
        setup["sources"] = {**setup["sources"], str(path): engine._sha256_file(path),
                            **project.prompt_sources(setup["prompt"])}
        setup["cases"] = [
            {**c, "prompt": inputs.render(without_cache_marker(setup["prompt"]["text"]), c["inputs"])}
            for c in setup["cases"]
        ]
    elif args.prompt_id:
        raise ValueError("--prompt-id requires --prompt")
    if prefix_path is not None:
        prefix_path = prefix_path.expanduser().resolve()
        setup["runner"]["prompt_cache_prefix"] = prefix_path.read_bytes().decode()
        setup["sources"] = {**setup["sources"], str(prefix_path): engine._sha256_file(prefix_path)}
    template_cache = cache_requested or (
        setup.get('cache_warmup', {}).get('prefix_source') == 'template'
        and not warmup and prefix_path is None and getattr(args, 'prompt_cache_key', None) is None)
    inherited_template_cache = bool(template_cache and args.from_experiment and not args.prompt
                                    and setup.get('cache_warmup', {}).get('prefix_source') == 'template')
    if template_cache:
        inherited = setup.get('cache_warmup', {})
        if inherited_template_cache:
            prefix = setup['runner']['prompt_cache_prefix']
            boundary = inherited.get('marker', inherited.get('first_placeholder'))
        else:
            prefixes = (template_prefix(setup['prompt']['text'], case['inputs']) for case in setup['cases'])
            prefix, boundary = next(prefixes)
            if any(candidate != prefix for candidate, _ in prefixes):
                raise ValueError('--prompt-cache declared prefix differs between cases; select cases sharing the declared context')
        if any(not case['prompt'].startswith(prefix) for case in setup['cases']):
            raise ValueError('--prompt-cache template prefix does not match every rendered prompt')
        setup['runner']['prompt_cache_prefix'] = prefix
    prefix = setup['runner'].get('prompt_cache_prefix')
    # Preserve frozen cache settings; prompt revisions select and validate a
    # new prefix while retaining the existing warmup workflow.
    if (prefix or template_cache) and not args.import_config:
        policy = (prefix_policy(prefix)
                  if (template_cache and not inherited_template_cache)
                  or prefix_path is not None or not args.from_experiment
                  else setup.get('cache_warmup'))
        if policy is not None and not policy['eligible']:
            raise ValueError(
                f"prompt_cache_prefix has {policy['estimated_prefix_tokens']:,} estimated "
                f"{policy['tokenizer']} tokens; minimum is {policy['minimum_prefix_tokens']:,}. "
                "Use a qualifying fixed prefix or prepare without prefix caching."
            )
        if template_cache:
            policy = {**policy, 'prefix_source': 'template',
                      **({'marker': boundary} if boundary == CACHE_PREFIX_END else {'first_placeholder': boundary})}
        if getattr(args, 'prompt_cache_key', None) is not None:
            setup.pop('cache_warmup', None)
        elif template_cache or prefix_path is not None or 'prompt_cache_key' not in setup['runner']:
            setup['runner']['prompt_cache_key'] = 'prefix-' + engine._sha256_bytes(prefix.encode())[:56]
            setup['cache_warmup'] = policy or prefix_policy(prefix)
    if warmup:
        prefix = setup["runner"].get("prompt_cache_prefix")
        if (not args.from_experiment or not prefix or args.only or args.prompt
                or args.prompt_id or args.baseline or args.import_config
                or overrides or prefix_path is not None):
            raise ValueError("--cache-warmup requires --from a native-prefix experiment without case, prompt or runner-setting overrides")
        setup["prompt"] = {**setup["prompt"], "id": setup["prompt"]["id"] + "-cache-warmup", "text": prefix}
        setup["dataset"] = {"adapter": "generic"}
        setup["cases"] = [{"id": "cache-warmup", "inputs": {}, "input_sha256": inputs.digest({}),
                           "prompt": prefix, "evidence": {"kind": "prefix-cache-warmup"}}]
        setup["preparation"] = {"version": "prompt-cache-warmup-v1", "cases": 1, "application_allowed": False}
        setup["runner"] = {**setup["runner"], "concurrency": 1}
        setup.pop('cache_warmup', None)
        baseline = None
    if args.import_config and (
        args.from_experiment or args.baseline or args.prompt or args.repeat or args.only or overrides
    ):
        raise ValueError(
            "--import-config requires an exact spec, without baseline, prompt override, repeat or --only"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".experiment-prepare-", dir=destination.parent
    ) as temporary:
        staged = Path(temporary)
        # Preserve the immediate baseline, not an ever-growing chain of old comparisons.
        snapshot = (
            None
            if baseline is None
            else {
                "experiment": baseline["experiment"],
                "prompt_id": baseline["prompt_id"],
                "cases": [
                    {k: v for k, v in c.items() if k not in {"baseline", "reviews"}}
                    for c in baseline["cases"]
                ],
            }
        )
        plan = {
            "schema_version": 1,
            **setup,
            "imported": bool(args.import_config),
            "baseline": snapshot,
            "baseline_prompt_id": baseline["prompt_id"] if baseline else None,
        }
        config_value = {
            "schema_version": 1,
            **setup["runner"],
            "run_directory": str(destination / "run"),
            "prompts": [
                {"prompt_id": c["id"], "text": c["prompt"]} for c in setup["cases"]
            ],
        }
        if args.import_config:
            config, manifest, cases = import_setup(args.import_config, setup)
            # Equality above verifies the schema; retain the recorded key order
            # as well, because native receipt identity hashes those exact bytes.
            if config.output_schema is not None:
                config_value["output_schema"] = config.output_schema
                plan["runner"] = {**plan["runner"], "output_schema": config.output_schema}
            config_value["run_directory"] = str(config.run_directory)
            config_value["prompts"] = [
                {"prompt_id": p.prompt_id, "text": p.text} for p in config.prompts
            ]
            plan["cases"] = cases
        write(staged / "runner-config.json", config_value)
        config = engine.PromptRunnerConfig.load(staged / "runner-config.json")
        for case in plan["cases"]:
            if "mechanical" in case:
                mechanical_result(plan["dataset"], case, config.output_schema)
        if not args.import_config:
            manifest = engine._build_prompt_manifest(
                config, engine._runner_version(config)
            )
            manifest["config_path"] = str(destination / "runner-config.json")
            if warmup:
                manifest['cache_warmup'] = True
            engine._request_history_options(
                config,
                manifest,
                repeat=args.repeat or warmup,
                history_root=args.history_root
                or (
                    Path(inherited_history) if inherited_history else destination.parent
                ),
            )
            (staged / "run").mkdir()
            engine._atomic_write(
                staged / "run" / "manifest.json", engine._canonical_json_bytes(manifest)
            )
        plan["run_directory"] = str(config.run_directory)
        plan["manifest_sha256"] = inputs.digest(manifest)
        plan["files"] = {
            "runner-config.json": engine._sha256_file(staged / "runner-config.json")
        }
        plan["code"] = {name: engine._sha256_file(path)
                        for name, path in project.code_files(plan['dataset']).items()}
        plan["requests_at_prepare"] = (
            request_preview(plan, manifest, plan["cases"])
            if include_request_preview and not args.import_config
            else None
        )
        write(staged / "experiment.json", {"sha256": inputs.digest(plan), "plan": plan})
        # Every artifact in this directory is generated, never reusable source.
        (staged / ".gitignore").write_text("*\n")
        # Reserve the destination exclusively; never replace someone else's experiment.
        destination.mkdir()
        for child in staged.iterdir():
            child.rename(destination / child.name)
    result = report(destination, include_request_preview=include_request_preview)
    write(destination / "report.json", result)
    return result


def import_setup(path, setup):
    """Import only a demonstrated template→inputs→saved-prompt mapping, never guess."""
    path = path.expanduser().resolve()
    value = read(path)
    if "prompts" in value:
        config = engine.PromptRunnerConfig.load(path)
    else:
        config, _, _ = engine._load_mask_batch(path)
    manifest = read(config.run_directory / "manifest.json")
    saved_prompts = {p["prompt_id"]: p["text"] for p in manifest["prompts"]}
    if saved_prompts != {p.prompt_id: p.text for p in config.prompts}:
        raise ValueError("legacy config does not match saved manifest prompts")
    for name in (
        "model",
        "reasoning_effort",
        "provider",
        "max_output_tokens",
        "timeout_seconds",
        "transport",
        "prompt_cache_key",
        "prompt_cache_prefix",
    ):
        default = {"timeout_seconds": 600, "transport": "sse"}.get(name)
        expected = setup["runner"].get(name, default)
        if expected != getattr(config, name) or expected != manifest.get(name, default):
            raise ValueError(f"legacy setting mismatch: {name}")
    if setup["runner"].get(
        "output_schema"
    ) != config.output_schema or config.output_schema != manifest.get(
        "response_schema"
    ):
        raise ValueError("legacy schema mismatch")
    by_prompt = {}
    for prompt in config.prompts:
        by_prompt.setdefault(prompt.text, []).append(prompt.prompt_id)
    cases = []
    for case in setup["cases"]:
        matches = by_prompt.get(case["prompt"], [])
        if case["id"] in matches:
            identifier = case["id"]
        elif case.get("evidence", {}).get("item_id") in matches:
            identifier = case["evidence"]["item_id"]
        elif len(matches) == 1:
            identifier = matches[0]
        else:
            raise ValueError(
                f"cannot verify unique saved input mapping for {case['id']}"
            )
        cases.append({**case, "job_id": identifier})
    if len({c["job_id"] for c in cases}) != len(config.prompts) or len(cases) != len(
        config.prompts
    ):
        raise ValueError("spec must account for every saved prompt exactly once")
    manifest_hash = inputs.digest(manifest)
    provenance = engine._load_runner_upgrades(
        config.run_directory,
        manifest_sha256=manifest_hash,
        initial_wrapper_sha256=manifest["wrapper_sha256"],
    )
    latest = engine._load_latest_results(
        config.run_directory / "results.jsonl", manifest_hash, provenance
    )
    for job in config.prompts:
        record = latest.get("prompt:" + job.prompt_id)
        if record and record["status"] == "succeeded":
            engine.validate_prompt_receipt(
                replace(config, prompts=(job,)), manifest, record
            )
    return config, manifest, cases


def _nonnegative_number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _uncached_cost(record, values, actual):
    model_result = record.get('model_result') or {}
    if 'uncached_catalog_cost_estimate_usd' in model_result:
        saved = model_result['uncached_catalog_cost_estimate_usd']
        return saved if _nonnegative_number(saved) else None
    if not values[1] and not values[2]:
        return actual
    # Older receipts sometimes retain enough SDK cost components to recover
    # their own input rate. Legacy input_tokens alone has ambiguous cache scope.
    usage = record['usage']
    cost = usage.get('cost', {})
    if (usage.get('input', 0) > 0 and all(_nonnegative_number(cost.get(k))
            for k in ('input', 'cacheRead', 'cacheWrite'))):
        baseline = actual - cost['cacheRead'] - cost['cacheWrite'] + (
            cost['input'] / usage['input']) * (values[1] + values[2])
        return baseline if _nonnegative_number(baseline) else None
    return None


def cost_statistics(observations):
    """API-equivalent cost on observed tokens, including warmup overhead."""
    priced = [cost for _, _, cost, _ in observations if cost is not None]
    comparisons = [o for o in observations if not o[3]]
    unknown_usage = sum(values is None for _, values, _, _ in comparisons)
    hits = sum(values is not None and values[1] > 0 for _, values, _, _ in comparisons)
    uncached = [_uncached_cost(record, values, cost) if values is not None and cost is not None else None
                for record, values, cost, _ in comparisons]
    complete = len(priced) == len(observations)
    total = math.fsum(priced) if complete else None
    baseline = math.fsum(uncached) if all(v is not None for v in uncached) else None
    savings = baseline - total if baseline is not None and total is not None else None
    return dict(
        basis='saved_catalog_estimates', requests=len(observations),
        requests_without_cost=len(observations) - len(priced),
        comparison_requests=len(comparisons), warmup_requests=len(observations) - len(comparisons),
        comparison_requests_without_usage=unknown_usage, cache_hit_requests=hits,
        comparison_requests_without_uncached_cost=sum(v is None for v in uncached),
        cache_hit_rate_percent=100 * hits / len(comparisons) if comparisons and not unknown_usage else None,
        total_cost_usd=total,
        mean_request_cost_usd=statistics.fmean(priced) if priced and complete else None,
        median_request_cost_usd=statistics.median(priced) if priced and complete else None,
        p95_request_cost_usd=sorted(priced)[math.ceil(0.95 * len(priced)) - 1] if priced and complete else None,
        uncached_comparison_cost_usd=baseline,
        net_cache_savings_usd=savings,
        net_cache_savings_percent=100 * savings / baseline if savings is not None and baseline > 0 else None,
    )


def usage_summary(calls, *, strict=True, warmup_calls=()):
    """Total saved usage; strict generation reports keep missing failures unknown."""
    fields = (
        ("input_tokens", "input", "input_tokens"),
        ("cached_input_tokens", "cacheRead", "cached_input_tokens"),
        ("cache_write_tokens", "cacheWrite", "cache_write_tokens"),
        ("output_tokens", "output", "output_tokens"),
        ("reasoning_tokens", "reasoning", "reasoning_output_tokens"),
    )
    usage = dict.fromkeys([f[0] for f in fields], 0)
    usage.update(attempts_without_usage=0, catalog_cost_estimate_usd=0,
                 attempts_without_cost_estimate=0, provider_reported_cost_usd=0,
                 attempts_without_provider_cost=0)
    observations = []
    for record, charge, warmup in [*((r, c, False) for r, c in calls),
                                  *((r, c, True) for r, c in warmup_calls)]:
        if type(charge) in (int, float) and math.isfinite(charge) and charge >= 0:
            usage["provider_reported_cost_usd"] += charge
        else:
            usage["attempts_without_provider_cost"] += 1
        saved = record.get("usage")
        values = [saved.get(sdk, saved.get(legacy, 0))
                  for _, sdk, legacy in fields] if isinstance(saved, dict) else []
        known = isinstance(saved, dict)
        if strict and known:
            known = (any(k in saved for k in ("input", "input_tokens"))
                     and any(k in saved for k in ("output", "output_tokens"))
                     and all(type(v) in (int, float) and math.isfinite(v) and v >= 0
                             for v in values)
                     and not (record.get("status") == "failed" and not any(values)))
            if known and not any(values):
                model = record.get('model_result') or {}
                raw = (model.get('response_metadata') or {}).get('usage') or {}
                if (model.get('usage_note', '').startswith('pi-ai')
                        and not (_nonnegative_number(raw.get('input_tokens', raw.get('prompt_tokens')))
                                 and _nonnegative_number(raw.get('output_tokens', raw.get('completion_tokens'))))):
                    known = False
        if not known:
            observations.append((record, None, None, warmup))
            usage["attempts_without_usage"] += 1
            usage["attempts_without_cost_estimate"] += 1
            continue
        if not strict:
            values = [v or 0 for v in values]
        for (field, _, _), value in zip(fields, values):
            usage[field] += value
        cost = saved.get("cost") or {}
        estimate = cost.get("total") if isinstance(cost, dict) else None
        observations.append((record, values, estimate if _nonnegative_number(estimate) else None, warmup))
        if estimate is None or (strict and not (
                type(estimate) in (int, float) and math.isfinite(estimate) and estimate >= 0)):
            usage["attempts_without_cost_estimate"] += 1
        else:
            usage["catalog_cost_estimate_usd"] += estimate
    if strict:
        usage["known_catalog_cost_estimate_usd"] = usage["catalog_cost_estimate_usd"]
        usage["known_provider_reported_cost_usd"] = usage["provider_reported_cost_usd"]
    if usage["attempts_without_cost_estimate"]:
        usage["catalog_cost_estimate_usd"] = None
    if usage["attempts_without_provider_cost"]:
        usage["provider_reported_cost_usd"] = None
    usage['cost_statistics'] = cost_statistics(observations)
    return usage


def generation_usage(config, manifest, selected, records, latest, files, reconciled_costs,
                     warmup_calls=(), warmup_sources=()):
    """Account for original calls once by verified file/line, not request text."""
    ledger = (config.run_directory / "results.jsonl").resolve()
    jobs = {"prompt:" + c.get("job_id", c["id"]): c for c in selected}
    source_ledgers = {ledger: records}
    source_attempts = {}
    originals = {}
    for number, record in enumerate(records, 1):
        case = jobs.get(record.get("job_key"))
        if case is None or record.get("execution") == "cross_run_reuse":
            continue
        if record.get("status") == "succeeded":
            job = engine.PromptJob(case.get("job_id", case["id"]), case["prompt"])
            engine.validate_prompt_receipt(replace(config, prompts=(job,)), manifest,
                                           record, files=files)
        originals[(ledger, number)] = (record, False)
    local_count = len(originals)
    for case in selected:
        record = latest.get("prompt:" + case.get("job_id", case["id"]))
        if not record or record.get("execution") != "cross_run_reuse":
            continue
        job = engine.PromptJob(case.get("job_id", case["id"]), case["prompt"])
        engine.validate_prompt_receipt(replace(config, prompts=(job,)), manifest,
                                       record, files=files)
        reference = record["reused_from"]
        source = engine._read_request_source(reference,
            engine._prompt_request_key(manifest, case["prompt"]), files=files)
        path = Path(reference["results_path"]).resolve()
        if path not in source_ledgers:
            source_manifest = files.json(path.parent / "manifest.json")
            source_provenance = files.provenance(path.parent,
                manifest_sha256=reference["manifest_sha256"],
                initial_wrapper_sha256=source_manifest["wrapper_sha256"])
            engine._load_latest_results(path, reference["manifest_sha256"], source_provenance)
            source_ledgers[path] = [json.loads(line) for line in files.lines(path)]
        if path not in source_attempts:
            by_job = {}
            for number, prior in enumerate(source_ledgers[path], 1):
                if prior.get("execution") != "cross_run_reuse":
                    by_job.setdefault(prior.get("job_key"), []).append((number, prior))
            source_attempts[path] = by_job
        for number, prior in source_attempts[path].get(source["job_key"], []):
            if number > reference["result_line"]:
                break
            if prior.get("status") == "succeeded":
                engine._read_request_source({**reference, "result_line": number,
                    "result_sha256": engine._sha256_bytes(files.line(path, number))},
                    engine._prompt_request_key(manifest, case["prompt"]), files=files)
            key = (path, number)
            if key in originals and originals[key][0] != prior:
                raise engine.RunnerError("generation source changed while reporting")
            originals.setdefault(key, (prior, True))
    reconciled = {config.run_directory.resolve(): reconciled_costs}
    calls, sources = [], []
    for (path, number), (record, added) in originals.items():
        line = files.line(path, number)
        if json.loads(line) != record:
            raise engine.RunnerError("generation source changed while reporting")
        if path.parent not in reconciled:
            reconciled[path.parent] = engine.load_reconciled_costs(
                path.parent, source_ledgers[path])
        calls.append((record, engine.provider_cost(record, reconciled[path.parent])))
        sources.append(dict(results_path=str(path), result_line=number,
                            result_sha256=engine._sha256_bytes(line),
                            job_key=record.get("job_key"), status=record.get("status"),
                            added_from_reuse=added))
    all_warmups = list(warmup_calls)
    sources.extend(warmup_sources)
    seen_warmups = {(s['results_path'], s['result_line']) for s in warmup_sources}
    inherited_warmups = 0
    # Tracing a reused generation must include the warmup paid by its original
    # batch, once even when several selected cases share that batch.
    for source_ledger in source_ledgers:
        source_directory = source_ledger.parent.parent
        if source_ledger == ledger or not (source_directory / 'cache-warmups.jsonl').exists():
            continue
        source_plan, source_config, _ = load(source_directory)
        prior_calls, prior_sources = cache_warmup_records(source_directory, source_plan, source_config)
        for call, source in zip(prior_calls, prior_sources):
            key = (source['results_path'], source['result_line'])
            if key not in seen_warmups:
                all_warmups.append(call)
                sources.append({**source, 'added_from_reuse': True})
                seen_warmups.add(key)
                inherited_warmups += 1
    # Instructions-only experiments contain warmup attempts, not comparisons.
    if manifest.get('cache_warmup'):
        all_warmups.extend(calls)
        calls = []
    return {**usage_summary(calls, strict=True, warmup_calls=all_warmups),
            "original_calls": len(calls) + len(all_warmups),
            "source_calls_added": len(originals) - local_count + inherited_warmups,
            "failed_calls": sum(r.get("status") == "failed" for r, _ in [*calls, *all_warmups]),
            "sources": sources}


def cache_warmup_records(directory, plan, config):
    """Verify separately preserved warmups and include their actual attempts."""
    index = directory / 'cache-warmups.jsonl'
    calls, sources = [], []
    seen = set()
    for entry in [json.loads(line) for line in index.read_bytes().splitlines()] if index.exists() else []:
        child = directory / 'cache-warmups' / entry['directory']
        if child.parent != directory / 'cache-warmups' or child in seen:
            raise ValueError('invalid cache warmup reference')
        seen.add(child)
        warm_plan, warm_config, warm_manifest = load(child)
        if (entry['parent_manifest_sha256'] != plan['manifest_sha256']
                or entry['manifest_sha256'] != warm_plan['manifest_sha256']
                or not warm_manifest.get('cache_warmup')
                or warm_plan['preparation'].get('application_allowed') is not False
                or len(warm_config.prompts) != 1
                or warm_config.prompts[0].text != config.prompt_cache_prefix
                or replace(warm_config, config_path=config.config_path,
                           run_directory=config.run_directory, prompts=config.prompts,
                           concurrency=config.concurrency) != config):
            raise ValueError('cache warmup does not match the frozen comparison settings')
        warm_report = report(child, generation_costs=True, include_request_preview=False)
        if warm_report['summary']['invalid_receipt']:
            raise ValueError('invalid cache warmup receipt')
        for source in warm_report['generation_usage']['sources']:
            path = Path(source['results_path'])
            raw = engine.ReceiptFiles().line(path, source['result_line'])
            if engine._sha256_bytes(raw) != source['result_sha256']:
                raise engine.RunnerError('cache warmup receipt changed while reporting')
            record = json.loads(raw)
            costs = engine.load_reconciled_costs(path.parent,
                [json.loads(line) for line in path.read_bytes().splitlines()])
            calls.append((record, engine.provider_cost(record, costs)))
            sources.append(source)
    return calls, sources


def report(directory, only=None, note_ids=None, *, generation_costs=False, include_request_preview=True):
    with report_scope():
        return _report(directory, only, note_ids, generation_costs=generation_costs,
                       include_request_preview=include_request_preview)


def _report(directory, only=None, note_ids=None, *, generation_costs=False, include_request_preview=True):
    directory = Path(directory).resolve()
    files = _REPORT_FILES.get()
    plan, config, manifest = load(directory)
    selected = plan["cases"]
    for requested, available, field in (
        (only, {c["id"] for c in selected}, "case"),
        (note_ids, {c.get("evidence", {}).get("note_id") for c in selected}, "note"),
    ):
        if requested is not None:
            if len(requested) != len(set(requested)) or set(requested) - available:
                raise ValueError(f"unknown or duplicate selected {field} ID")
    if only is not None:
        selected = [c for c in selected if c["id"] in only]
    if note_ids is not None:
        selected = [c for c in selected if c.get("evidence", {}).get("note_id") in note_ids]
    selected_jobs = {"prompt:" + c.get("job_id", c["id"]) for c in selected}
    manifest_hash = inputs.digest(manifest)
    provenance = files.provenance(
        config.run_directory,
        manifest_sha256=manifest_hash,
        initial_wrapper_sha256=manifest["wrapper_sha256"],
    )
    ledger = config.run_directory / "results.jsonl"
    evidence = files.ledger(ledger) if ledger.exists() else None
    latest = engine._load_latest_results(ledger, manifest_hash, provenance,
        evidence=None if evidence is None else zip(evidence['lines'], evidence['records']))
    records = evidence['records'] if evidence is not None else []
    reconciled_costs = engine.load_reconciled_costs(config.run_directory, records)
    reviews_path = directory / "reviews.jsonl"
    comments_path = directory / "ui-comments.json"
    comments = (
        inputs.prominence_comments(read(comments_path))
        if plan["dataset"]["adapter"] in {"prominence", "generic"} and comments_path.exists()
        else {}
    )
    reviews = (
        files.ledger(reviews_path)['records']
        if reviews_path.exists()
        else []
    )
    ui_path = directory / "review-ui.json"
    if ui_path.exists():
        for binding in read(ui_path):
            session_path = directory / binding["session_path"]
            if session_path.exists():
                reviews.extend(
                    inputs.ui_reviews(
                        binding,
                        read(directory / binding["batch_path"]),
                        read(session_path),
                    )
                )
    baseline = (
        {c["id"]: c for c in plan["baseline"]["cases"]} if plan["baseline"] else {}
    )
    cases, comparisons = [], Counter()
    for case in selected:
        job = engine.PromptJob(case.get("job_id", case["id"]), case["prompt"])
        record = latest.get("prompt:" + job.prompt_id)
        row = {**case, "status": "unrun", "output": None, "baseline": None}
        if record:
            row["elapsed_seconds"] = record.get("elapsed_seconds")
            model_result = record.get('model_result') or {}
            for field in ('response_metadata', 'usage', 'stop_reason'):
                if field in model_result:
                    row[field] = model_result[field]
            row["receipt"] = {
                "results_path": str(ledger),
                "record_sha256": inputs.digest(record),
            }
            row["reused"] = record.get("execution") == "cross_run_reuse"
            if record["status"] == "failed":
                row.update(
                    status="failed",
                    error=record.get("error"),
                    failure_kind=engine.prompt_failure_kind(record),
                    output=record.get("final_output_raw"),
                )
            else:
                row["output"] = record.get("final_output_raw")
                try:
                    engine.validate_prompt_receipt(
                        replace(config, prompts=(job,)), manifest, record, files=files
                    )
                except engine.RunnerError as error:
                    row.update(status="invalid_receipt", error=str(error))
                else:
                    try:
                        row.update(
                            inputs.validate(
                                plan["dataset"],
                                case,
                                record["final_output_raw"],
                                config.output_schema,
                            )
                        )
                    except (ValueError, engine.RunnerError) as error:
                        row.update(status="malformed", error=str(error))
        elif uses_mechanical(manifest, case):
            row.update(mechanical_result(plan["dataset"], case, config.output_schema))
        prior = baseline.get(case["id"])
        if prior is None:
            row["comparison"] = "missing_baseline"
        elif prior["inputs"] != case["inputs"]:
            row["comparison"] = "input_mismatch"
        else:
            row["baseline"] = {
                k: v
                for k, v in prior.items()
                if k not in {"baseline", "reviews", "evidence"}
            }
            if prior["status"] not in {"valid", "empty"} or row["status"] not in {
                "valid",
                "empty",
            }:
                row["comparison"] = "not_comparable"
            else:
                row["comparison"] = (
                    "unchanged" if row["output"] == prior["output"] else "changed"
                )
        row["reviews"] = [
            {
                **r,
                "current": r["input_sha256"] == case["input_sha256"]
                and r["output_sha256"] == inputs.digest(row["output"]),
            }
            for r in reviews
            if r["case_id"] == case["id"]
        ]
        row["reviews"].extend(
            {"case_id": case["id"], "role": "user", "verdict": "comment",
             "comment": comment["comment"], "source": comments_path.name,
             "current": key == inputs.prominence_comment_key(row)}
            for key, comment in comments.items()
            if comment["note_id"] == case["id"] and comment["comment"]
        )
        comparisons[row["comparison"]] += 1
        cases.append(row)
    attempts = [r for r in records if r.get("execution") != "cross_run_reuse"
                and r.get("job_key") in selected_jobs]
    warmup_calls, warmup_sources = cache_warmup_records(directory, plan, config)
    comparison_calls = [(r, engine.provider_cost(r, reconciled_costs)) for r in attempts]
    usage = usage_summary([] if manifest.get('cache_warmup') else comparison_calls,
                          warmup_calls=[*warmup_calls, *(comparison_calls if manifest.get('cache_warmup') else [])])
    summary = {
        s: sum(c["status"] == s for c in cases)
        for s in ("valid", "empty", "malformed", "invalid_receipt", "failed", "unrun")
    }
    summary.update(
        cases=len(cases),
        attempts=len(attempts) + len(warmup_calls),
        reused=sum(c.get("reused", False) for c in cases),
    )
    if any("mechanical" in case for case in selected):
        summary["mechanical"] = sum(c.get("origin") == "mechanical" for c in cases)
    result = {
        "experiment": str(directory),
        "prompt_id": plan["prompt"]["id"],
        "settings": {
            **plan["runner"],
            "timeout_seconds": config.timeout_seconds,
            "max_output_tokens": config.max_output_tokens,
            "transport": config.transport,
            "concurrency": config.concurrency,
        },
        "imported": plan["imported"],
        "adapter": plan["dataset"]["adapter"],
        "summary": summary,
        "usage": usage,
        "comparison": dict(comparisons),
        "cases": cases,
        "baseline_only": sorted(set(baseline) - {c["id"] for c in cases}),
    }
    from .scheduling import bound_group, snapshot
    result['scheduling'] = snapshot(config.run_directory)
    result['scheduling'].update(valid=summary['valid'], failed=summary['failed'], reused=summary['reused'])
    result['scheduling'].setdefault('scheduling_group',
        plan.get('scheduling_group') or bound_group(config.run_directory))
    if plan.get('cache_warmup') or warmup_calls:
        result['cache_warmup'] = {**plan.get('cache_warmup', {}),
                                  'attempts': len(warmup_calls),
                                  'usage': usage_summary([], warmup_calls=warmup_calls),
                                  'sources': warmup_sources}
    execution_settings = directory / 'execution-settings.jsonl'
    if execution_settings.exists():
        with execution_settings.open() as f:
            result['execution_settings'] = [json.loads(line) for line in f]
        if any(r['manifest_sha256'] != manifest_hash for r in result['execution_settings']):
            raise ValueError('execution settings belong to another manifest')
    result["requests"] = (
        request_preview(plan, manifest, cases) if include_request_preview and not plan["imported"] else None
    )
    if result["adapter"] == "bounded_comparisons":
        token_counts = [case["evidence"]["input_tokens"] for case in cases]
        result["prepared_inputs"] = dict(
            tokenizer="o200k_base", batches=len(cases),
            comparisons=sum(len(case["evidence"]["pairs"]) for case in cases),
            rendered_input_tokens=sum(token_counts),
            min_rendered_input_tokens=min(token_counts, default=0),
            max_rendered_input_tokens=max(token_counts, default=0))
    if result["adapter"] == "masks":
        result["posts"] = len({c["evidence"]["note_id"] for c in cases})


    selected_extension = project.extension()
    if selected_extension:
        selected_extension.enrich_report(result, plan, cases)
    if generation_costs:
        result["generation_usage"] = generation_usage(
            config, manifest, selected, records, latest, files, reconciled_costs,
            warmup_calls, warmup_sources)
    return result


def collect(directories, output, worker_config=None):
    selected = project.extension()
    if selected is None:
        raise ValueError("this command requires a project extension")
    return selected.collect(directories, output, worker_config)


def execution_upgrade(directory, plan, reason):
    """Audit an explicitly approved code transition without rewriting frozen inputs."""
    active = plan["code"]
    available = project.code_files(plan["dataset"])
    current = {name: engine._sha256_file(available[name]) for name in active}
    path = directory / "code-upgrades.jsonl"
    results_path = Path(plan["run_directory"]) / "results.jsonl"
    results = results_path.read_bytes() if results_path.exists() else b""
    for line in path.read_bytes().splitlines() if path.exists() else []:
        entry = json.loads(line)
        if (entry["from"] != active or set(entry["to"]) != set(active)
                or entry["results_prefix_bytes"] > len(results)
                or entry["results_prefix_bytes"] < 0
                or engine._sha256_bytes(results[:entry["results_prefix_bytes"]]) != entry["results_prefix_sha256"]):
            raise ValueError("invalid execution code upgrade history")
        active = entry["to"]
    if current != active:
        if not reason or not reason.strip():
            from .compatibility import OPERATIONAL_FILES
            changed = {name for name in active if current[name] != active[name]}
            if not all(name in OPERATIONAL_FILES and available[name].resolve() ==
                       Path(engine.__file__).parent / name for name in changed):
                raise ValueError("experiment code changed; inspect it and supply --runner-upgrade-reason, or prepare a new experiment")
            reason = 'compatible runner workflow update; frozen inputs and request identity preserved'
        engine._append_json_line(path, {
            "from": active, "to": current, "reason": reason,
            "results_prefix_bytes": len(results),
            "results_prefix_sha256": engine._sha256_bytes(results),
        })
    return reason


def reconcile_costs(directory):
    """Read missing OpenRouter charges; never rewrite original model receipts."""
    directory = Path(directory).resolve()
    with locked(directory):
        plan, config, manifest = load(directory)
        if config.provider != 'openrouter' or plan['imported']:
            raise ValueError('cost lookup requires a native OpenRouter experiment')
        report(directory)  # Verify saved experiment and receipt provenance first.
        path = config.run_directory / 'results.jsonl'
        records = [json.loads(line) for line in path.read_bytes().splitlines()] if path.exists() else []
        costs = engine.load_reconciled_costs(config.run_directory, records)
        pending = [r for r in records if r.get('execution') != 'cross_run_reuse'
                   and engine.provider_cost(r, costs) is None
                   and (r.get('model_result') or {}).get('response_metadata', {}).get('id')]
        if pending:
            ids = list(dict.fromkeys(r['model_result']['response_metadata']['id'] for r in pending))
            node = shutil.which('node')
            argv = ([node] if node else ['nix', 'shell', 'nixpkgs#nodejs_24', '--command', 'node'])
            argv += [str(Path(__file__).with_name('frontier_runner') / 'billing.mjs')]
            result = subprocess.run(argv, input=json.dumps(ids), text=True, capture_output=True,
                                    timeout=60 + 35 * len(ids))
            if result.returncode:
                raise ValueError('cost lookup failed; original reserves retained')
            rows = json.loads(result.stdout)
            if not isinstance(rows, list) or len(rows) != len(ids):
                raise ValueError('invalid cost lookup response')
            by_id = {r.get('id'): r for r in rows}
            if set(by_id) != set(ids):
                raise ValueError('cost lookup generation IDs differ')
            for row in rows:
                if 'error' not in row:
                    cost = row.get('total_cost')
                    if type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0:
                        raise ValueError('invalid cost lookup charge')
            for record in pending:
                row = by_id[record['model_result']['response_metadata']['id']]
                if 'error' in row:
                    continue  # Missing bills remain explicitly unknown.
                engine._append_json_line(config.run_directory / 'cost-receipts.jsonl', {
                    'source': 'openrouter/generation', 'record_sha256': inputs.digest(record),
                    'generation': row, 'looked_up_at_utc': datetime.now(timezone.utc).isoformat(),
                })
        return report(directory)


def run(directory, max_new_attempts=None, runner_upgrade_reason=None, *,
        max_cost_usd=None, max_request_cost_usd=None, attempt_budget_path=None,
        additional_attempt_budget_paths=(), include_request_preview=True,
        only=None, warm_cache=True, keep_retrying_provider_errors=False, scheduling_group=None):
    directory = directory.resolve()
    if (directory / 'bounded-build.json').exists():
        if (any(x is not None for x in (max_cost_usd, max_request_cost_usd, attempt_budget_path, only, scheduling_group))
                or additional_attempt_budget_paths or not warm_cache or keep_retrying_provider_errors):
            raise ValueError('bounded builds use their frozen shared attempt budget and subscription route')
        run_build = project.import_module('bounded_hierarchy_build').run_build
        return run_build(directory, max_new_attempts, runner_upgrade_reason)
    with locked(directory):
        plan, config, manifest = load(directory)
        if plan["imported"]:
            raise ValueError(
                "imported experiments are read-only; prepare --from this experiment to run a candidate"
            )
        from . import scheduling
        group = scheduling.validate_group(scheduling_group)
        if group is not None and plan.get('scheduling_group') not in (None, group):
            raise ValueError('scheduling group differs from the prepared experiment')
        group = scheduling.bind_group(config.run_directory, group or plan.get('scheduling_group'))
        cases = {c["id"]: c for c in plan["cases"]}
        selected = None if only is None else set(only)
        if selected is not None and (len(only) != len(selected) or selected - cases.keys()):
            raise ValueError('unknown or duplicate selected case ID')
        jobs = tuple(job for job in config.prompts
                     if (selected is None or job.prompt_id in selected)
                     and not uses_mechanical(manifest, cases[job.prompt_id]))
        if not jobs:
            execution_upgrade(directory, plan, runner_upgrade_reason)
            result = report(directory, include_request_preview=include_request_preview)
            write(directory / "report.json", result)
            return result
        identity = engine._runner_identity(config)
        if not engine.compatibility.compatible(manifest, identity):
            raise ValueError("model runner changed since preparation")
        engine._validate_auth_file(config.auth_file)
        runner_upgrade_reason = execution_upgrade(directory, plan, runner_upgrade_reason)
        engine.compatibility.upgrade_worker(config.run_directory, manifest, identity)
        engine._append_json_line(directory / 'execution-settings.jsonl', dict(
            manifest_sha256=inputs.digest(manifest), concurrency=scheduling.LIMIT,
            scheduling='shared-fair-queue-v2', scheduling_group=group,
            prepared_concurrency=config.concurrency, only=only, warm_cache=warm_cache,
            keep_retrying_provider_errors=keep_retrying_provider_errors,
            started_at_utc=datetime.now(timezone.utc).isoformat()))
        provenance = engine._load_runner_upgrades(
            config.run_directory, manifest_sha256=inputs.digest(manifest),
            initial_wrapper_sha256=manifest["wrapper_sha256"],
        )
        wrapper_changed = provenance.active_wrapper_sha256 != engine._sha256_file(Path(engine.__file__))
        error = None
        # Resolve the selected project validator before entering worker threads.
        validate = inputs.validate

        def validate_output(job, raw):
            validate(
                plan["dataset"], cases[job.prompt_id], raw, config.output_schema
            )

        warmup_lock = threading.Lock()
        warmup_checked = False
        warmup_error = None
        context = copy_context()

        def warm_before_dispatch(budget):
            nonlocal warmup_checked, warmup_error
            with warmup_lock:
                if warmup_error is not None:
                    raise warmup_error
                if warmup_checked:
                    return
                # Keep room for a real comparison. Persistent admission is
                # rechecked on every launch, including the warmup.
                if budget.remaining() < 2:
                    warmup_checked = True
                    return
                def warm():
                    parent = directory / 'cache-warmups'
                    parent.mkdir(exist_ok=True)
                    number = len(list(parent.iterdir())) + 1
                    child = parent / f'{number:04d}'
                    args = SimpleNamespace(destination=child, from_experiment=directory,
                        spec=None, baseline=None, only=None, prompt=None, prompt_id=None,
                        import_config=None, repeat=True, history_root=Path(manifest['request_reuse']['history_root']),
                        cache_warmup=True, prompt_cache_prefix=None,
                        **{k: None for k in ('model', 'reasoning_effort', 'provider', 'concurrency',
                            'max_output_tokens', 'transport', 'runner_binary', 'prompt_cache_key')})
                    prepare(args, include_request_preview=False)
                    warm_plan, warm_config, warm_manifest = load(child)
                    engine._append_json_line(directory / 'cache-warmups.jsonl', dict(
                        directory=child.name, parent_manifest_sha256=plan['manifest_sha256'],
                        manifest_sha256=warm_plan['manifest_sha256']))
                    try:
                        engine._execute_prompt_batch(warm_config, warm_manifest, retry_failed=False,
                            new_job_budget=budget,
                            request_queue=queue,
                            keep_retrying_provider_errors=keep_retrying_provider_errors,
                            validate_output=lambda job, raw: inputs.validate(
                                {'adapter': 'generic'}, warm_plan['cases'][0], raw, config.output_schema))
                    finally:
                        write(child / 'report.json', report(child, include_request_preview=False))
                try:
                    queue = scheduling.active_queue()
                    queue.warming(True)
                    try:
                        context.run(warm)
                    finally:
                        queue.warming(False)
                except (engine.RunnerError, ValueError) as caught:
                    warmup_error = caught
                    raise
                warmup_checked = True

        try:
            engine._execute_prompt_batch(
                replace(config, prompts=jobs),
                manifest,
                retry_failed=False,
                validate_output=validate_output,
                max_new_attempts=max_new_attempts,
                max_cost_usd=max_cost_usd,
                max_request_cost_usd=max_request_cost_usd,
                attempt_budget_path=attempt_budget_path,
                additional_attempt_budget_paths=additional_attempt_budget_paths,
                keep_retrying_provider_errors=keep_retrying_provider_errors,
                runner_upgrade_reason=runner_upgrade_reason if wrapper_changed else None,
                before_new_attempt=(warm_before_dispatch if warm_cache and plan.get('cache_warmup', {}).get('eligible') else None),
                scheduling_group=group,
            )
        except engine.RunnerError as caught:
            error = caught
        finally:
            result = report(directory, include_request_preview=include_request_preview)
            write(directory / "report.json", result)
        if error:
            raise engine.RunnerError(
                f"{error}; partial results: {directory / 'report.json'}"
            ) from error
        return result


def review(args):
    selected = project.extension()
    if selected:
        return selected.review(args)
    if args.ui:
        raise ValueError('review UI requires a project extension')
    with locked(args.experiment.resolve()):
        result = report(args.experiment)
        if not all((args.case, args.role, args.verdict, args.comment)):
            raise ValueError('recording review requires --case, --role, --verdict and --comment')
        case = next((c for c in result['cases'] if c['id'] == args.case), None)
        if case is None:
            raise ValueError('unknown case')
        engine._append_json_line(args.experiment / 'reviews.jsonl', {
            'case_id': args.case, 'role': args.role, 'verdict': args.verdict,
            'comment': args.comment, 'time': datetime.now(timezone.utc).isoformat(),
            'input_sha256': case['input_sha256'], 'output_sha256': inputs.digest(case['output'])})
        write(args.experiment / 'report.json', report(args.experiment))
        print(f'Recorded {args.role} review for {args.case}. Model calls made: 0')


def compact(result, *, prepared=False):
    print(f"{'Prepared' if prepared else 'Experiment'}: {result['experiment']}")
    print(f"Prompt: {result['prompt_id']}")
    settings = result["settings"]
    print(
        f"Model: {settings['provider']} / {settings['model']}; reasoning: {settings['reasoning_effort']}"
    )
    cache = result.get('cache_warmup', {})
    if cache.get('prefix_source') == 'template':
        boundary = cache.get('marker', cache.get('first_placeholder'))
        print(f"Caching: prefix before {boundary}; "
              f"{cache['estimated_prefix_tokens']:,} estimated {cache['tokenizer']} tokens "
              f"(minimum {cache['minimum_prefix_tokens']:,}); automatic warming configured.")
    available = sum(
        c["baseline"] is not None and c["baseline"]["status"] in {"valid", "empty"}
        for c in result["cases"]
    )
    print(f"Saved baseline results available: {available}/{len(result['cases'])}")
    if result["adapter"] == "masks":
        print(f"Posts: {result['posts']}; subjects/calls: {result['summary']['cases']}")
    print("Cases: " + ", ".join(f"{k}={v}" for k, v in result["summary"].items()))
    schedule = result.get('scheduling')
    if schedule:
        print(f"Queue: {schedule['state']}; queued={schedule['queued']} running={schedule['running']}; "
              f"global={schedule.get('global_running', 0)}/{schedule['global_limit']}; "
              f"wait={schedule.get('wait_reason') or '-'}; "
              f"group={schedule.get('scheduling_group') or 'independent'}; "
              f"group_running={schedule.get('groups', {}).get(schedule.get('group'), {}).get('running', 0)}")
    if "prepared_inputs" in result:
        print("Offline input sizing (rendered prompt proxy, not provider billing): "
              + json.dumps(result["prepared_inputs"]))
    print("Comparison (text changes, not quality): " + json.dumps(result["comparison"]))
    if result["baseline_only"]:
        print(
            "Baseline-only cases (not in this experiment): "
            + ", ".join(result["baseline_only"])
        )
    if result["requests"]:
        print(
            f"Requests remaining: {result['requests']['new']} new, {result['requests']['reusable']} reusable"
        )
    if prepared:
        print("Model calls made: 0")
    else:
        print(
            "Usage (new attempts only; reasoning already included in output): "
            + json.dumps({k: v for k, v in result["usage"].items() if k != 'cost_statistics'})
        )
        stats = result['usage'].get('cost_statistics')
        if stats is not None:
            def number(value):
                return 'unknown' if value is None else format(value, '.6g')
            print('API-equivalent request cost (USD): ' + '; '.join(
                f'{label}={number(stats[field])}' for label, field in (
                    ('total', 'total_cost_usd'), ('mean', 'mean_request_cost_usd'),
                    ('median', 'median_request_cost_usd'), ('p95', 'p95_request_cost_usd'))))
            print(f"Estimated net cache savings: {number(stats['net_cache_savings_usd'])} USD "
                  f"({number(stats['net_cache_savings_percent'])}%); "
                  f"comparison cache hits: {stats['cache_hit_requests']}/{stats['comparison_requests']} "
                  f"({number(stats['cache_hit_rate_percent'])}%)")
        if "generation_usage" in result:
            print("Generation usage (unique original calls; includes traced reused sources): "
                  + json.dumps({k: v for k, v in result["generation_usage"].items()
                                if k != "sources"}))
    for case in result["cases"]:
        print(f"  {case['id']}: {case['status']}; {case['comparison']}")
    action = "show" if result["imported"] or not result["summary"]["unrun"] else "run"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        *(["--project-root", str(project.root())] if project.root() else []),
        action,
        result["experiment"],
    ]
    if action == "show":
        command += ["--case", result["cases"][0]["id"]]
    print("Next: " + shlex.join(command))
    if action == "run":
        print(
            "Inference requires user authorization; preparation is not authorization."
        )


def score_review(args):
    selected = project.extension()
    if selected is None:
        raise ValueError("this command requires a project extension")
    return selected.score_review(args)


def parser():
    submission_help = """Model batch submission:
  Launch all ready, authorized model batches immediately.
  Start their run processes concurrently, before waiting for completion.
  The shared cap limits active model requests, not run processes.
  Do not add outer batch caps, semaphores, fixed-size thread pools, or waves.
  A cap on batch coordinators is also an outer batch cap: it hides ready work.
  The queue cannot schedule a batch until its run process has started.
  run blocks until its own batch finishes; prepare alone does not submit work.
  Wait for genuine input dependencies and validate the required results.
  Start newly ready batches without waiting for unrelated batches to finish.
  Keep local CPU/GPU encoding limits separate from provider-batch submission.
"""
    queue_help = submission_help + """
  Concurrent submission example (two prepared, authorized independent batches):
    ../experiment-runner/bin/experiment-runner run target/a-v1 > target/a-v1/run.log 2>&1 &
    batch_a_pid=$!
    ../experiment-runner/bin/experiment-runner run target/b-v1 > target/b-v1/run.log 2>&1 &
    batch_b_pid=$!
    batch_a_status=0
    wait "$batch_a_pid" || batch_a_status=$?
    batch_b_status=0
    wait "$batch_b_pid" || batch_b_status=$?
  Both run processes start before either wait. Check both statuses and reports
  before starting dependent work. Use your authorized attempt/spending limits.

Internal request queue:
  All experiments by this user on this host share at most 30 active requests.
  Capacity is automatic: no concurrency flag or runner.concurrency in new specs.
  Historical concurrency fields stay frozen but do not control dispatch.
  Share slots between overall experiments, then between their batches.
  Prepare all related batches with the same --scheduling-group ID, or use the
  top-level scheduling_group field in the spec. IDs are 1-64 letters, digits,
  dots, underscores or hyphens, starting with a letter or digit. They identify
  the effort, not a model, prompt, prefix, batch or agent. More batches in one
  group do not increase its share. Ungrouped batches have independent shares.
  prepare --from inherits the ID, including subsets and warmups. Resumes retain
  membership; conflicting frozen/saved IDs are refused before inference.
  For a legacy ungrouped batch, run --scheduling-group ID binds membership once
  in run/scheduling-group.json without changing old plans, requests or receipts.
  Ready groups share freed slots fairly; unused shares can be borrowed.
  When endpoint capacity permits, one group can use 30, two sustained groups balance
  near 15 each, three near 10. Fixed native endpoint limits can reduce admission;
  a serial local endpoint admits one request across experiments using it.
  Endpoint metadata is discovered without inference before worker launch.
  Distinct endpoints and provider requests remain eligible for global slots.
  Running requests finish normally; a newcomer receives the next freed slots.
  Account/transport backoff and gradual recovery can reduce capacity below 30.
  Shared backoff needs rate/overload, valid Retry-After or connection evidence.
  Ordinary stream/server errors delay only that case's retry; fresh work runs.
  Active requests finish normally. Late in-flight failures share one cooldown
  generation, except explicit Retry-After deadlines. Recovery starts at one
  request and doubles after successful completions; new pressure can slow it.
  Every verified temporary provider failure retries automatically with per-case
  2/4/8/.../60s exponential backoff until success or an authorized stop.
  There is no two-attempt cutoff. This includes HTTP 408/409/425/429/5xx,
  SSE/WebSocket server/rate/overload errors and verified network interruptions.
  Honor longer valid Retry-After/retry-after-ms; malformed values fall back.
  Saved attempt counts survive resume. Attempts/spending caps, pause and cancel
  still apply. Local, validation, request/length, fatal and unknown errors stop.
  Authentication/exhausted-credit errors are fatal, including within HTTP 200.
  OpenRouter's explicit in-flight-budget 402 with a valid wait deadline retries;
  ordinary 402 does not. Explicit server no-retry instructions are respected.
  Old receipts do not recreate an outage. No retry flag is required;
  --keep-retrying-provider-errors remains a compatibility option with no effect.
  Measure successful completions/minute, failures and retry cost.
  Workers launch only after admission; queued cases consume no attempt budget.
  Warmups and retries share the same slots/budget. Reused answers need neither.
  Eligible prefixes warm before comparisons, within the run's fair share.
  Frozen prompts, schemas and native cache identity remain unchanged.

  run prints live counts and wait reasons on stderr. Inspect another terminal:
    experiment-runner show EXPERIMENT --json
  scheduling reports queued/ready/running/valid/failed/reused, global_running out of
  global_limit: 30, and wait_reason (capacity, fair_share, account_backoff,
  transport_backoff, provider_backoff, recovery_limit, prefix_warmup or ready).
  scheduling_group identifies explicit membership; group identifies its effective
  allocation. groups contains per-group batches/running/queued/ready/eligible_ready.
  Progress reports group and group_running. Group counts include warmups and retries.
  provider_backoff is a batch's shared-pressure delay.
  queue_wait_seconds is the longest admission wait, excluding worker execution
  and earlier preparation. running_elapsed_seconds is the oldest current
  admission's elapsed time. cooldown_until_utc is a known cooldown deadline,
  not a provider ETA. Interrupted admissions are cleaned by process identity;
  saved receipts and conservative attempt reservations remain authoritative.
  queued includes unfinished cases preparing, waiting for warming or retry time.
  ready counts requests at admission; eligible_ready excludes pause, backoff,
  recovery limits and occupied endpoint capacity, but includes global capacity
  and fairness waiting. global_ready
  and global_eligible_ready sum across experiments. Ready warmups count once.
  Timestamped history: EXPERIMENT/run/scheduling.jsonl, beside scheduling.json.
  Each observation retains observed_at_utc, monotonic_seconds, boot_id, counts
  and all active experiments' wait/pause reasons from one coherent atomic state read.
  Startup/ready/admitted/released/cancelled/warmup/backoff/finished transitions
  and periodic progress are retained; resume appends without rewriting history.
  For empty slots, inspect global_running < 30 with global_eligible_ready > 0
  and measure time until admission or eligibility changes. Brief startup is
  not proof of a stall. Sort by monotonic_seconds within one boot_id; concurrent
  appends can arrive out of order. No prompt, output, credential or scope IDs
  are stored. Loaded coordinators gain history on their next individual restart.
  This controls runner traffic, not the provider's cap or guaranteed chat capacity.

  Updates and resume:
    experiment-runner pause EXPERIMENT
    experiment-runner show EXPERIMENT --json
    experiment-runner run EXPERIMENT --attempt-budget SAME_BUDGET
  pause stops new admission and drains active requests; other experiments run.
  Wait for scheduling.state=paused, then run again with the same limits/budget.
  Compatible scheduling/backoff changes resume the same directory automatically;
  completed receipts and exact reuse survive. Compatible live workers continue;
  incompatible changes request a handover before new dispatch. Coordinators
  retain loaded workflow code until individually paused and restarted.
  Older loaded coordinators do not implement group fairness. Updating the launcher
  alone cannot regroup their active batches; resume the parent workflow and its
  drained child batches using the same saved inputs, limits and authorization.
  Changed request-producing code still
  requires a newly prepared experiment; --runner-upgrade-reason cannot bypass it.
  Project adapter/evaluator changes still require --runner-upgrade-reason.
  Processes started with older runners lack handover support and need a one-time
  finish/restart before updating their bound source; no global idle is required.
"""
    root = argparse.ArgumentParser(
        prog="experiment-runner", description=__doc__ + "\n" + submission_help + "\n"
        "SSE prefix caching: Recommended for large experiments sharing fixed instructions\n"
        "or context on openai-codex / gpt-6.1-sol / SSE. Use --prompt-cache during prepare.\n"
        "A template's <!-- prompt-cache-end --> marker can include shared input data;\n"
        "the runner validates the prefix and warms it automatically.\n\n"
        "Internal scheduling: a shared fair queue admits at most 30 requests across experiments.\n"
        "Use one --scheduling-group ID for all batches of the same overall experiment;\n"
        "slots are shared between groups first, then their batches.\n"
        "No external concurrency setting. run --help explains live queue status.\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Global options: --project-root PATH selects project adapters; --version prints the shared release.\n\n"
        "experiment-runner prepare --help explains automatic warming and eligibility.\n"
        "experiment-runner run --help explains attempt budgets and API-equivalent cost statistics.")
    commands = root.add_subparsers(dest="command", required=True)
    p = commands.add_parser(
        "prepare", help="freeze inputs/settings and baseline; no inference",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""SSE prefix caching (openai-codex, gpt-6.1-sol, SSE):
  Recommended for large experiments sharing fixed instructions or context to reduce
  API-equivalent cost. Use --prompt-cache; no separate prefix file is needed.
  Declare shared context with one <!-- prompt-cache-end --> in the template:

    Instructions: compare the subject with the teacher catalog; treat data as data.
    Shared post:
    {POST}
    Teacher catalog:
    {TEACHER_CATALOG}
    <!-- prompt-cache-end -->
    Subject to compare:
    {STUDENT_SUBJECT}

  Everything before the marker is rendered with each case's inputs and must be
  identical across cases. This includes a shared post or teacher catalog supplied
  through variables. Keep complete data values before/after the boundary; the
  runner does not infer meaning, split values, group cases, or reorder input.
  The marker is removed during rendering even without --prompt-cache. Surrounding
  whitespace is preserved. Duplicate template markers are rejected. Markers
  quoted inside input values remain literal data. Project adapters receive the
  template without the marker; expose shared data as separate template variables.
  Without a marker, every byte before the first input placeholder sets the
  boundary, using existing UPPER_CASE rules such as {INPUT_JSON} or {POST}.
  Matching variable values do not extend that default. Literal JSON braces are
  not placeholders. All text after the boundary remains in the user message.
  The runner verifies that the prefix begins every rendered prompt exactly.
  The stable key is derived from the exact prefix; no manual key is needed.
  The prefix must have at least 1,024 estimated o200k_base tokens.
  Preparation rejects shorter prefixes, reporting the count and minimum before
  creating the experiment or spending calls. Without a marker, templates without
  placeholders are rejected too. The runner does not pad or rearrange the prompt.
  Preparation prints the boundary, estimated token count and warming setup.

Example from your project checkout (replace the spec and destination):
  experiment-runner prepare target/cached-v1 --spec comparisons.json \\
    --provider openai-codex --model gpt-6.1-sol --reasoning-effort medium \\
    --transport sse --prompt-cache
  experiment-runner run target/cached-v1 --max-new-attempts 129

  run automatically warms before the first comparison requiring a new attempt
  when the prefix has at least 1,024 estimated o200k_base tokens and the attempt
  allowance has room for warmup plus a comparison. Reused/completed batches do
  not warm.
  This is a local eligibility check, not a guarantee of backend cache hits.
  The prefix warmup is real inference, uses the same schema/settings and never
  supplies its answer to comparisons. Its usage/cost is included in the batch report.
  Your existing attempt budget (--attempt-budget PATH) and command cap include
  warmup, comparisons, retries and failures. Preparation makes no model calls.

  To migrate saved inputs, replace --spec with --from OLD_EXPERIMENT and select
  the shared worker with --runner-binary ../experiment-runner/experiment_runner/run-model.sh.
  Otherwise --from retains the old frozen worker. --from retains the frozen
  prefix/key/eligibility even with --only or --repeat; it does not infer a longer
  prefix from a subset. Changing the template with --prompt revalidates its
  prefix and recomputes its count and key. Old manual cache plans retain their
  settings unless --prompt-cache is supplied.
  Specs can set runner.prompt_cache to true (a JSON boolean).
  New specs without caching keep the ordinary request format. Adding caching
  changes request identity, so uncached answers are not reused as cached answers.
  Adapter-provided mechanical results need no model call and appear in show.
  Identical requests reuse answers; --repeat requests fresh trials
  and disables mechanical routing, executing the original prompts.
  WebSocket cache options are refused. See experiment-runner run --help for costs."""
    )
    p.add_argument("destination", type=Path)
    start = p.add_mutually_exclusive_group(required=True)
    start.add_argument("--spec", type=Path)
    start.add_argument("--from", dest="from_experiment", type=Path)
    p.add_argument("--baseline", type=Path)
    p.add_argument("--only", action="append", metavar="CASE_ID",
                   help="prepare only this case (repeat for several); preserves exact inputs and matching baseline")
    p.add_argument("--prompt", type=Path)
    p.add_argument("--prompt-id")
    p.add_argument("--repeat", action="store_true")
    p.add_argument("--history-root", type=Path)
    p.add_argument('--scheduling-group', metavar='ID',
                   help='share one scheduling allocation across all batches of this overall experiment; inherited by --from')
    p.add_argument("--model")
    p.add_argument("--reasoning-effort")
    p.add_argument("--provider", choices=("openai-codex", "openrouter", "local"))
    p.add_argument("--runner-binary", type=Path,
                   help="explicitly select a worker for this new experiment, including when inheriting an older run")
    p.add_argument("--max-output-tokens", type=int)
    p.add_argument("--transport", choices=("sse", "websocket", "http"),
                   help="explicit transport; websocket is subscription-only, without fallback")
    p.add_argument("--prompt-cache-key",
                   help=argparse.SUPPRESS)
    caching = p.add_mutually_exclusive_group()
    caching.add_argument('--prompt-cache', action='store_true',
                         help='cache the declared template prefix (or text before its first variable) and warm automatically; recommended for large experiments')
    caching.add_argument("--prompt-cache-prefix", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--cache-warmup", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument(
        "--import-config",
        type=Path,
        help="verify/import a historical batch without running it",
    )
    p = commands.add_parser(
        "run",
        help="execute a prepared experiment; retry only temporary provider errors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=queue_help + "\nSSE prefix caching:\n" + """
  Enable with prepare --prompt-cache; see prepare --help.
  Mechanical results skip dispatch, warming and attempt budgets; mixed batches
  execute only residual jobs. Explicit prepare --repeat disables that routing.
  run automatically sends a prefix warmup before new comparisons
  when its frozen prefix is eligible and there is room for warmup + comparison.
  All-reused/completed batches and insufficient allowances skip warming.
  A resume with new comparisons can warm again; each invocation records its
  warmup separately. A failed warmup stops that invocation before comparisons.
  Every warmup, retry and failed attempt consumes --max-new-attempts and the
  same --attempt-budget PATH. With one slot left, the comparison takes priority.

  Inspect usage with experiment-runner show EXPERIMENT --json or report.json.
  Batch totals INCLUDE warmup input, cached input, cache writes and output;
  cache_warmup separates its usage and native receipt references for inspection.
  Output already includes reasoning; do not charge it twice.
  Missing failed-call usage leaves the complete cost unknown. Saved receipts
  retain raw provider usage. Cache hits are not guaranteed.

  Reports show total/mean/median/p95 API-equivalent request cost and comparison
  cache-hit rate. p95 uses nearest rank; distributions include warmup and retries.
  Estimated net caching savings = (uncached comparison cost - total actual cost)
  / uncached comparison cost * 100. The uncached estimate prices the same observed
  input/output tokens without cache reads or writes and excludes warmup. Actual
  cost includes warmup, cache writes, retries and output (reasoning included).
  Negative savings are possible. This is not a separate uncached experiment or
  subscription billing. Missing usage/pricing keeps affected statistics unknown.
  show --json exposes usage.cost_statistics; --generation-costs traces original
  requests once, so answer reuse is not counted as a cache hit or new savings.""",
    )
    p.add_argument("experiment", type=Path)
    p.add_argument('--scheduling-group', metavar='ID',
                   help='bind an older ungrouped batch to an overall experiment; saved for subsequent resumes')
    p.add_argument('--only', action='append', metavar='CASE_ID',
                   help='dispatch only these frozen cases; the full report retains unselected cases')
    p.add_argument('--keep-retrying-provider-errors', action='store_true',
                   help='compatibility option; verified temporary provider errors already retry automatically')
    p.add_argument("--max-new-attempts", type=int,
                   help="cap newly sent attempts, including provider retries; reserve spending for this allowance")
    p.add_argument("--attempt-budget", type=Path,
                   help="persistent cross-experiment launch counter; failed/interrupted attempts are not refunded")
    p.add_argument("--max-cost-usd", type=float,
                   help="cumulative experiment spending allowance; includes prior attempts, never truncates responses")
    p.add_argument("--max-request-cost-usd", type=float,
                   help="caller-verified maximum cost reserved for each in-flight call; required with --max-cost-usd")
    p.add_argument("--runner-upgrade-reason", help="approve project adapter/evaluator upgrades; compatible runner updates are automatic, incompatible requests remain refused")
    p = commands.add_parser('pause', help='stop new admissions for one run; active requests finish normally',
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=queue_help)
    p.add_argument('experiment', type=Path)
    p = commands.add_parser('reconcile-costs', help='lookup missing OpenRouter charges; no inference')
    p.add_argument('experiment', type=Path)
    p = commands.add_parser(
        "show", help="inspect all cases, or exact case evidence; no inference",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=queue_help + "\nCost statistics:\n" + """
  The batch view prints API-equivalent total/mean/median/p95 request cost,
  comparison cache-hit rate and estimated net dollars/percent saved including
  warmup overhead. --json exposes usage.cost_statistics. Estimates use saved
  catalog pricing and observed tokens; they are not subscription billing or a
  matched uncached experiment. Unknown usage/pricing is not treated as zero.
  --generation-costs includes original reused calls and their warmup once.
  See run --help for the savings formula and accounting details.""",
    )
    p.add_argument("experiment", type=Path)
    p.add_argument("--case")
    p.add_argument("--prompt-only", action="store_true", help="print the exact rendered prompt; requires --case")
    p.add_argument("--only", action="append", metavar="CASE_ID",
                   help="limit cases and usage to these IDs (repeatable; read-only)")
    p.add_argument("--note", action="append", metavar="NOTE_ID",
                   help="limit cases and usage to saved evidence.note_id (repeatable)")
    p.add_argument("--generation-costs", action="store_true",
                   help="trace verified original generation costs, counting reused sources once; no inference")
    p.add_argument('--inputs-only', action='store_true',
                   help='show only IDs, exact inputs and input hashes; hide prompts, answers and review evidence')
    p.add_argument("--json", action="store_true")
    p = commands.add_parser(
        "revise",
        help="create a version from exact edits and print its full diff",
        description='No model calls. Edits JSON: {"edits":[{"old":"exact original text","new":"replacement"}]}. '
        "Optional base_sha256 pins the original; otherwise it is computed. "
        "Matches must be unique and nonoverlapping. Original bytes are preserved; "
        "destinations cannot exist. Prints only the full-context diff to stdout.",
    )
    p.add_argument("base", type=Path)
    p.add_argument("destination", type=Path)
    p.add_argument("--edits", required=True, help="JSON file or - for stdin")
    p = commands.add_parser(
        "review", help="record attributed judgments or open an existing task UI"
    )
    p.add_argument("experiment", type=Path)
    p.add_argument("--case")
    p.add_argument("--role", choices=("agent", "user"))
    p.add_argument("--verdict", choices=("accept", "reject", "uncertain"))
    p.add_argument("--comment")
    p.add_argument("--ui", action="store_true")
    p.add_argument("--view", choices=("prominence",),
                   help="render saved generic prominence diagnostics in the existing mask viewer")
    p.add_argument("--node", action="append", metavar="NODE_ID",
                   help="prominence UI: show all masks of this node; repeat for several")
    p.add_argument("--ordering", help="start in a named saved cosine-comparison ordering")
    p.add_argument("--check", action="store_true",
                   help="validate the prominence UI batch without opening a terminal")
    p.add_argument("--flagged-only", action="store_true",
                   help="title/mask/prominence UI: current reject/uncertain reviews and unresolved failures")
    p.add_argument("--only", action="append", metavar="CASE_ID",
                   help="title/mask/prominence UI: show this case (repeat for several); intersects --flagged-only")
    p.add_argument("--ui-binary", type=Path)
    p.add_argument("--cosine-comparison", type=Path,
                   help="saved prominence_cosine/centroid comparison; adds offline ranking orders")
    selected = project.extension()
    if selected:
        selected.add_commands(commands)
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        selected = project.extension()
        handled = selected.handle_command(args) if selected else NotImplemented
        if handled is not NotImplemented:
            return handled
        if args.command == 'prepare':
            if args.from_experiment and args.baseline:
                raise ValueError('--from already specifies the baseline')
            compact(prepare(args), prepared=True)
        elif args.command == 'run':
            if args.max_new_attempts is not None and args.max_new_attempts < 1:
                raise ValueError('--max-new-attempts must be positive')
            if args.runner_upgrade_reason is not None and (not args.runner_upgrade_reason.strip()):
                raise ValueError('--runner-upgrade-reason must not be blank')
            result = run(args.experiment, args.max_new_attempts, args.runner_upgrade_reason, max_cost_usd=args.max_cost_usd, max_request_cost_usd=args.max_request_cost_usd, attempt_budget_path=args.attempt_budget, only=args.only, keep_retrying_provider_errors=args.keep_retrying_provider_errors, scheduling_group=args.scheduling_group)
            if (args.experiment / 'bounded-build.json').exists():
                print(json.dumps(result, indent=2))
                return int(result.get('status') != 'finished')
            compact(result)
            if result.get('scheduling', {}).get('state') == 'paused':
                return 0
            return int(any(case['status'] in ('malformed', 'invalid_receipt', 'failed', 'unrun')
                           for case in result['cases'] if args.only is None or case['id'] in args.only))
        elif args.command == 'pause':
            from .scheduling import pause
            _, config, _ = load(args.experiment.resolve())
            print(json.dumps(pause(config.run_directory), indent=2))
        elif args.command == 'reconcile-costs':
            compact(reconcile_costs(args.experiment))
        elif args.command == 'show':
            if args.inputs_only:
                if args.note or args.generation_costs or args.prompt_only:
                    raise ValueError('--inputs-only supports --case/--only, without evidence, cost or prompt filters')
                plan, _, _ = load(args.experiment)
                requested = set(args.only or []) | ({args.case} if args.case else set())
                available = {case['id'] for case in plan['cases']}
                if requested - available:
                    raise ValueError('unknown case')
                cases = [{key: case[key] for key in ('id', 'inputs', 'input_sha256')}
                         for case in plan['cases'] if not requested or case['id'] in requested]
                print(json.dumps(cases[0] if args.case and not args.only else {'cases': cases},
                                 ensure_ascii=False, indent=2))
                return 0
            if args.prompt_only and (not args.case):
                raise ValueError('--prompt-only requires --case')
            if (args.experiment / 'bounded-build.json').exists():
                if any((args.case, args.only, args.note, args.generation_costs)):
                    raise ValueError('inspect individual cases in the child experiments')
                report_build = project.import_module('bounded_hierarchy_build').report_build
                print(json.dumps(report_build(args.experiment), indent=2))
                return 0
            only = [args.case] if args.generation_costs and args.case and (args.only is None) else args.only
            result = report(args.experiment, only, args.note, generation_costs=args.generation_costs)
            if args.case:
                case = next((c for c in result['cases'] if c['id'] == args.case), None)
                if case is None:
                    raise ValueError('unknown case')
                result = {'settings': result['settings'], **case, **({'generation_usage': result['generation_usage']} if args.generation_costs else {})}
            if args.prompt_only:
                print(result['prompt'], end='')
            elif args.json or args.case:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                compact(result)
        elif args.command == 'revise':
            try:
                request = json.load(sys.stdin) if args.edits == '-' else read(args.edits)
            except (ValueError, UnicodeError) as error:
                raise ValueError(f'could not read edits JSON: {error}') from error
            if 'base_sha256' not in request:
                request['base_sha256'] = engine._sha256_file(args.base)
            sys.stdout.write(engine.revise_prompt(args.base, args.destination, request))
            print(f'Created {args.destination}; provenance: {args.destination}.revision.json', file=sys.stderr)
        else:
            review(args)
        return 0
    except (ValueError, KeyError, TypeError, OSError, engine.RunnerError) as error:
        print(f'Experiment failed: {error}', file=sys.stderr)
        return 1


if __name__ == "__main__":
    from experiment_runner.cli import main as cli_main
    raise SystemExit(cli_main())
