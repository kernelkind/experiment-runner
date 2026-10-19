"""Read-only integrity checks for native prompt experiments after relocation."""
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path

from . import extraction_pipeline as engine


class Locations:
    """Resolve saved paths without changing identities or falling back to originals."""

    def __init__(self, path):
        path = Path(path).resolve()

        def unique_pairs(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f'duplicate location key: {key}')
                result[key] = value
            return result

        value = json.loads(path.read_text(), object_pairs_hook=unique_pairs)
        if (not isinstance(value, dict) or value.get('schema_version') != 1
                or set(value) != {'schema_version', 'roots', 'files'}):
            raise ValueError('locations must contain schema_version: 1, roots and files')
        self.roots, self.files = {}, {}
        for name, table in (('roots', self.roots), ('files', self.files)):
            if not isinstance(value[name], dict):
                raise ValueError(f'locations.{name} must be an object')
            for original, destination in value[name].items():
                original = self.original(original)
                if original in table:
                    raise ValueError(f'duplicate location: {original}')
                if not isinstance(destination, str) or not destination:
                    raise ValueError('location destinations must be nonempty paths')
                table[original] = (path.parent / destination).resolve()

    @staticmethod
    def original(value):
        path = Path(value)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError(f'saved location must be absolute without ..: {path}')
        return path

    def __call__(self, path):
        path = self.original(path)
        if path in self.files:
            return self.files[path]
        for parent in (path, *path.parents):
            if parent in self.roots:
                return self.roots[parent] / path.relative_to(parent)
        raise ValueError(f'no extracted location for {path}; add it to the locations file')


def verify(directory, locations=None):
    """Check frozen bindings and every successful native receipt; never execute saved code.

    Receipt verification is not an evaluation of answer quality or proof that a
    ledger is complete. Archive checksums remain the completeness reference.
    Failed attempts get the ledger's provenance/sequence checks, not success checks.
    """
    directory = Path(directory).resolve()
    local = engine.ReceiptFiles()
    files = engine.ReceiptFiles(locate=Locations(locations) if locations else Path)
    envelope = local.json(directory / 'experiment.json')
    plan = envelope['plan']
    if engine._sha256_json(plan) != envelope['sha256']:
        raise ValueError('prepared experiment hash differs')
    if plan['schema_version'] != 1:
        raise ValueError('unsupported experiment schema')
    for name, expected in plan['files'].items():
        if Path(name).is_absolute() or '..' in Path(name).parts:
            raise ValueError(f'prepared file must be inside the experiment: {name}')
        if local.sha256(directory / name) != expected:
            raise ValueError(f'prepared file hash differs: {name}')
    if 'runner-config.json' not in plan['files']:
        raise ValueError('prepared experiment does not bind runner-config.json')
    config = engine.PromptRunnerConfig.load(directory / 'runner-config.json', files=local, saved=True)
    if str(config.run_directory) != plan['run_directory']:
        raise ValueError('prepared run directory differs from config')
    manifest = files.json(config.run_directory / 'manifest.json')
    if engine._sha256_json(manifest) != plan['manifest_sha256']:
        raise ValueError('prepared run manifest hash differs')
    if manifest.get('mode') != 'prompt':
        raise ValueError('verify supports native prompt experiments')
    for path, expected in plan['sources'].items():
        if files.sha256(Path(path)) != expected:
            raise ValueError(f'source hash differs: {path}')

    jobs = {p.prompt_id: p for p in config.prompts}
    cases = {}
    for case in plan['cases']:
        job_id = case.get('job_id', case['id'])
        if job_id in cases or case['prompt'] != jobs[job_id].text:
            raise ValueError(f'case prompt binding differs: {case["id"]}')
        if engine._sha256_json(case['inputs']) != case['input_sha256']:
            raise ValueError(f'case input hash differs: {case["id"]}')
        cases[job_id] = case
    if set(cases) != set(jobs):
        raise ValueError('case IDs differ from configured prompts')
    if manifest['prompts'] != [{'prompt_id': p.prompt_id, 'text': p.text} for p in config.prompts]:
        raise ValueError('manifest prompts differ from config')

    manifest_hash = files.sha256(config.run_directory / 'manifest.json')
    provenance = files.provenance(config.run_directory, manifest_sha256=manifest_hash,
                                 initial_wrapper_sha256=manifest['wrapper_sha256'])
    files.worker_provenance(config.run_directory, manifest)
    ledger = config.run_directory / 'results.jsonl'
    latest, verified, reused, failed = {}, 0, 0, 0
    if files.locate(ledger).exists():
        evidence = ((line, json.loads(line)) for line in files.lines(ledger))
        for number, _, record in engine._iter_results(ledger, manifest_hash, provenance, evidence=evidence):
            job_id = record['job_key'].removeprefix('prompt:')
            if record['job_key'] != 'prompt:' + job_id or job_id not in jobs:
                raise ValueError(f'unknown prompt at results line {number}')
            latest[job_id] = record['status']
            if record['status'] == 'failed':
                failed += 1
                continue
            try:
                engine.validate_prompt_receipt(replace(config, prompts=(jobs[job_id],)),
                                               manifest, record, files=files)
            except engine.RunnerError as error:
                raise ValueError(f'results line {number}: {error}') from error
            verified += 1
            reused += int(record.get('execution') == 'cross_run_reuse')
    counts = Counter(succeeded=0, failed=0, unrun=0, mechanical=0)
    for job_id, case in cases.items():
        mechanical = 'mechanical' in case and not manifest.get('request_reuse', {}).get('intentional_repeat', False)
        counts[latest.get(job_id, 'mechanical' if mechanical else 'unrun')] += 1
    return {'integrity': 'verified', 'experiment_sha256': envelope['sha256'],
            'sources_checked': len(plan['sources']), 'verified_receipts': verified,
            'reused_receipts': reused, 'failed_attempts': failed, 'cases': dict(counts)}


def summary(result):
    counts = result['cases']
    return (f"Integrity checks passed. Sources checked: {result['sources_checked']}. "
            f"Successful receipts: {result['verified_receipts']} "
            f"(reused: {result['reused_receipts']}).\n"
            f"Latest cases: {counts['succeeded']} succeeded, {counts['failed']} failed, "
            f"{counts['unrun']} unrun, {counts['mechanical']} mechanical.\n"
            f"Failed attempts recorded: {result['failed_attempts']}. "
            "Answer quality and mechanical decisions were not evaluated.")
