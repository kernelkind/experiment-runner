"""Project saved prompt answers into compact, portable publication files.

This exports an existing report snapshot. It neither runs models nor follows
original input/receipt paths, and it does not claim receipt verification.
"""
from collections import Counter
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import re
import tempfile

import ijson
from ijson.common import JSONError, ObjectBuilder

from . import extraction_pipeline as engine, json_projection
from .publication_files import write_publication


FORMAT = 'prompt-publication-v1'
_HEX = re.compile(r'[0-9a-f]{64}')
_SETTINGS = {'provider', 'model', 'reasoning_effort', 'max_output_tokens',
             'timeout_seconds', 'transport', 'concurrency'}
_MODEL_RUNNER = {'provider', 'model', 'reasoning_effort', 'runner_version',
                 'runner_sha256', 'request_code_sha256', 'wrapper_sha256',
                 'response_schema_sha256'}
_PLAN = {'prompt', 'dataset', 'runner', 'sources', 'code', 'publication_role',
         'manifest_sha256', 'request_reuse', 'model_runner', 'baseline_prompt_id', 'imported'}
_HEADERS = {'prompt_id', 'adapter', 'imported', 'settings', 'comparison'}
_CASE = {'id', 'inputs', 'input_sha256', 'status', 'output', 'reused', 'origin',
         'attempt', 'attempts', 'comparison', 'reviews', 'failure_kind', 'receipt',
         'request_sha256', 'prompt_sha256', 'reused_from', 'mechanical_sha256'}
_REVIEW = {'role', 'verdict', 'comment', 'time', 'input_sha256', 'output_sha256', 'current'}
_STATUSES = {'valid', 'empty', 'malformed', 'invalid_receipt', 'failed', 'unrun'}


def _scalars(value, fields):
    return {key: value[key] for key in sorted(fields) if key in value
            and (value[key] is None or type(value[key]) in (str, int, float, bool))}


def _value(events, first, keep):
    """Build one selected value; discarded containers only advance events."""
    builder = ObjectBuilder() if keep else None
    event, value = first
    depth = 0
    while True:
        if builder is not None:
            builder.event(event, value)
        if event in ('start_map', 'start_array'):
            depth += 1
        elif event in ('end_map', 'end_array'):
            depth -= 1
        if not depth:
            return builder.value if builder is not None else None
        event, value = next(events)


def _mapping(events, fields):
    result = {}
    for event, key in events:
        if event == 'end_map':
            return result
        if event != 'map_key':
            raise ValueError('expected saved JSON object fields')
        value = _value(events, next(events), key in fields)
        if key in fields:
            result[key] = value
    raise ValueError('incomplete saved JSON object')


def _end(events):
    if next(events, None) is not None:
        raise ValueError('saved JSON has trailing content')


def _binding(digest, case):
    identifier, input_hash = case.get('id'), case.get('input_sha256')
    if (not isinstance(identifier, str) or not identifier
            or not isinstance(input_hash, str) or not _HEX.fullmatch(input_hash)):
        raise ValueError('saved case requires its ID and input hash')
    digest.update(engine._canonical_json_bytes([identifier, input_hash]))


def _plan_fields(events):
    result, seen_cases = {}, False
    bindings = hashlib.sha256()
    for event, key in events:
        if event == 'end_map':
            if not seen_cases:
                raise ValueError('saved plan has no case input bindings')
            result['_case_bindings_sha256'] = bindings.hexdigest()
            return result
        if event != 'map_key':
            raise ValueError('invalid saved plan object')
        first = next(events)
        if key == 'cases':
            if seen_cases or first != ('start_array', None):
                raise ValueError('saved plan requires one cases array')
            seen_cases = True
            for first in events:
                if first == ('end_array', None):
                    break
                if first != ('start_map', None):
                    raise ValueError('saved plan cases must be objects')
                _binding(bindings, _mapping(events, {'id', 'input_sha256'}))
            else:
                raise ValueError('incomplete saved plan cases')
        else:
            value = _value(events, first, key in _PLAN)
            if key in _PLAN:
                result[key] = value
    raise ValueError('incomplete saved plan object')


def _plan(stream):
    events = iter(ijson.basic_parse(stream, use_float=True))
    envelope = {}
    if next(events) != ('start_map', None):
        raise ValueError('saved experiment must be a JSON object')
    for event, key in events:
        if event == 'end_map':
            _end(events)
            return envelope
        if event != 'map_key':
            raise ValueError('invalid saved experiment envelope')
        first = next(events)
        if key == 'plan':
            if first != ('start_map', None):
                raise ValueError('saved experiment plan must be an object')
            envelope[key] = _plan_fields(events)
        elif key == 'sha256':
            envelope[key] = _value(events, first, True)
        else:
            _value(events, first, False)
    raise ValueError('incomplete saved experiment envelope')


def _cases(stream, header):
    events = iter(ijson.basic_parse(stream, use_float=True))
    if next(events) != ('start_map', None):
        raise ValueError('saved report must be a JSON object')
    seen_cases = False
    for event, key in events:
        if event == 'end_map':
            _end(events)
            if not seen_cases:
                raise ValueError('saved report has no cases array')
            return
        if event != 'map_key':
            raise ValueError('invalid saved report object')
        first = next(events)
        if key == 'cases':
            if seen_cases or first != ('start_array', None):
                raise ValueError('saved report requires one cases array')
            seen_cases = True
            for first in events:
                if first == ('end_array', None):
                    break
                if first != ('start_map', None):
                    raise ValueError('saved report cases must be objects')
                yield _mapping(events, _CASE)
            else:
                raise ValueError('incomplete saved report cases')
        else:
            value = _value(events, first, key in _HEADERS)
            if key in _HEADERS:
                header[key] = value
    raise ValueError('incomplete saved report object')


class _HashedInput:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    def read(self, size):
        value = self.stream.read(size)
        self.digest.update(value)
        return value


def _stamp(path):
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _unchanged(path, before):
    if _stamp(path) != before:
        raise ValueError('saved publication source changed during export: '+path.name)


def _hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for value in iter(lambda: stream.read(65536), b''):
            digest.update(value)
    return digest.hexdigest()


@contextmanager
def saved_snapshot(directory):
    """Read an inactive experiment without adding locks to old source records."""
    path = Path(directory).resolve()/'.experiment.lock'
    if not path.exists():
        yield
        return
    with path.open('rb') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('experiment is active; export after it stops') from error
        yield


def _code(value):
    return {name: digest for name, digest in value.items()
            if isinstance(name, str) and '\\' not in name and not Path(name).is_absolute()
            and '..' not in Path(name).parts and isinstance(digest, str) and _HEX.fullmatch(digest)}


def _metadata(directory, plan, identity, experiment_hash):
    prompt, dataset = plan.get('prompt', {}), plan.get('dataset', {})
    text = prompt.get('text')
    reuse = plan.get('request_reuse', {}).get('intentional_repeat')
    return dict(schema_version=1, format=FORMAT, source_id=identity,
        experiment_id=directory.name, publication_role=plan.get('publication_role'),
        prompt=dict(id=prompt.get('id'), sha256=(hashlib.sha256(text.encode()).hexdigest()
                                               if isinstance(text, str) else None)),
        dataset=dict(adapter=dataset.get('adapter'),
                     source_sha256=plan.get('sources', {}).get(dataset.get('path'))),
        settings={}, code=dict(sources=_code(plan.get('code', {})),
                               model_runner=_scalars(plan.get('model_runner', {}), _MODEL_RUNNER)),
        request_reuse=dict(intentional_repeat=reuse if type(reuse) is bool else None),
        baseline_prompt_id=plan.get('baseline_prompt_id'), imported=plan.get('imported'),
        snapshot=dict(experiment_sha256=experiment_hash, report_sha256=None,
            case_bindings_sha256=plan.get('_case_bindings_sha256'),
            manifest_sha256=plan.get('manifest_sha256'), sidecar_freshness='not_checked',
            receipt_validation='not_performed',
            note='Saved report snapshot; later ledger or review-sidecar changes are not incorporated.'),
        outcome={})


def _row(case):
    variables = case.get('inputs')
    if not isinstance(variables, dict):
        raise ValueError('saved case inputs must be an object')
    input_hash = engine._sha256_json(variables)
    if case.get('input_sha256') != input_hash:
        raise ValueError('saved case input hash differs')
    identifier, status, output = case.get('id'), case.get('status'), case.get('output')
    if not isinstance(identifier, str) or not identifier:
        raise ValueError('saved case ID must be nonempty')
    if status not in _STATUSES or not (output is None or isinstance(output, str)):
        raise ValueError('saved case has an unsupported answer or status')
    row = dict(case_id=identifier, input_sha256=input_hash, status=status, output=output,
               output_sha256=engine._sha256_json(output),
               reused=case.get('reused') if type(case.get('reused')) is bool else None)
    row.update(_scalars(case, {'origin', 'attempt', 'attempts', 'comparison', 'failure_kind',
                              'request_sha256', 'prompt_sha256', 'mechanical_sha256'}))
    if isinstance(case.get('receipt'), dict):
        row['receipt'] = _scalars(case['receipt'], {'record_sha256'})
    if isinstance(case.get('reused_from'), dict):
        row['reused_from'] = _scalars(case['reused_from'],
                                      {'result_line', 'result_sha256', 'manifest_sha256'})
    reviews = case.get('reviews', [])
    if not isinstance(reviews, list) or any(not isinstance(review, dict) for review in reviews):
        raise ValueError('saved case reviews must be a list of objects')
    row['reviews'] = [_scalars(review, _REVIEW) for review in reviews]
    return row, variables


def export_results(directory, output=None, *, result=None, plan=None, automatic=False):
    """Publish a saved report or the report already produced by normal execution.

    Explicit exports may select unmarked historical plans. Automatic exports
    require a frozen standalone role; known internal jobs are always excluded.
    Normal callers hold the experiment lock. Offline CLI callers also use
    saved_snapshot. Only compact final rows are spooled, never rendered requests.
    """
    if automatic and plan is not None and plan.get('publication_role') != 'standalone':
        return None
    directory = Path(directory).expanduser().resolve()
    output = Path(output).expanduser() if output is not None else directory/'publication'
    resolved = output.resolve()
    if resolved == directory or resolved in directory.parents:
        raise ValueError('publication must not replace a source directory')
    experiment_path, report_path = directory/'experiment.json', directory/'report.json'
    experiment_stamp = _stamp(experiment_path)
    try:
        if plan is None:
            with experiment_path.open('rb') as stream:
                reader = _HashedInput(stream)
                envelope = _plan(reader)
                experiment_hash = reader.digest.hexdigest()
            plan, identity = envelope['plan'], envelope['sha256']
        else:
            identity = engine._sha256_json(plan)
            experiment_hash = _hash(experiment_path)
            bindings = hashlib.sha256()
            for case in plan['cases']:
                _binding(bindings, case)
            plan = dict(plan, _case_bindings_sha256=bindings.hexdigest())
        _unchanged(experiment_path, experiment_stamp)
    except (JSONError, StopIteration, UnicodeError, OverflowError, KeyError) as error:
        raise ValueError('invalid saved experiment snapshot') from error
    role = plan.get('publication_role')
    if role not in (None, 'standalone', 'internal'):
        raise ValueError('invalid frozen publication role')
    if automatic and role != 'standalone':
        return None
    if role == 'internal':
        raise ValueError('internal experiment results belong to their parent publication')
    if not isinstance(identity, str) or not _HEX.fullmatch(identity):
        raise ValueError('saved experiment has no valid frozen plan identity')
    if not report_path.is_file():
        raise ValueError('saved report.json is required; missing case outcomes are not reconstructed')
    report_stamp = _stamp(report_path)
    manifest_path, manifest_stamp, manifest_hash = directory/'run/manifest.json', None, None
    if manifest_path.is_file() and ('request_reuse' not in plan or 'model_runner' not in plan):
        manifest_stamp = _stamp(manifest_path)
        with manifest_path.open('rb') as stream:
            reader = _HashedInput(stream)
            manifest = json_projection.project(reader, {'request_reuse', *_MODEL_RUNNER})
            manifest_hash = reader.digest.hexdigest()
        _unchanged(manifest_path, manifest_stamp)
        plan = dict(plan)
        plan.setdefault('request_reuse', manifest.get('request_reuse', {}))
        plan.setdefault('model_runner', _scalars(manifest, _MODEL_RUNNER))
    metadata = _metadata(directory, plan, identity, experiment_hash)
    if manifest_hash is not None:
        metadata['snapshot']['local_manifest_sha256'] = manifest_hash
    header, counts = {}, Counter(dict.fromkeys([*sorted(_STATUSES), 'cases', 'reused', 'mechanical'], 0))
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.prompt-publication-', dir=output.parent) as temporary:
        spool = Path(temporary)/'results.jsonl'

        def input_rows():
            seen_inputs, seen_cases = set(), set()
            report_bindings = hashlib.sha256()
            with spool.open('w', encoding='utf-8') as results:
                with report_path.open('rb') as stream:
                    reader = _HashedInput(stream)
                    if result is None:
                        cases = _cases(reader, header)
                    else:
                        header.update({key: result[key] for key in _HEADERS if key in result})
                        cases = iter(result['cases'])
                    try:
                        for case in cases:
                            row, variables = _row(case)
                            if row['case_id'] in seen_cases:
                                raise ValueError('saved report has duplicate case IDs')
                            seen_cases.add(row['case_id'])
                            _binding(report_bindings, case)
                            counts['cases'] += 1
                            counts[row['status']] += 1
                            counts['reused'] += row['reused'] is True
                            counts['mechanical'] += row.get('origin') == 'mechanical'
                            json.dump(row, results, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
                            results.write('\n')
                            if row['input_sha256'] not in seen_inputs:
                                seen_inputs.add(row['input_sha256'])
                                yield dict(input_sha256=row['input_sha256'], inputs=variables)
                        if result is not None:
                            for _ in iter(lambda: reader.read(65536), b''):
                                pass
                    except (JSONError, StopIteration, UnicodeError, OverflowError) as error:
                        raise ValueError('invalid saved report snapshot') from error
                    metadata['snapshot']['report_sha256'] = reader.digest.hexdigest()
            _unchanged(experiment_path, experiment_stamp)
            _unchanged(report_path, report_stamp)
            if manifest_stamp is not None:
                _unchanged(manifest_path, manifest_stamp)
            if report_bindings.hexdigest() != plan['_case_bindings_sha256']:
                raise ValueError('saved report input selection differs from the frozen plan')
            if header.get('prompt_id', metadata['prompt']['id']) != metadata['prompt']['id']:
                raise ValueError('report prompt ID differs from the frozen plan')
            settings = {**plan.get('runner', {}), **header.get('settings', {})}
            metadata['settings'] = _scalars(settings, _SETTINGS)
            if isinstance(settings.get('output_schema'), dict):
                # Schema property order participates in native request identity;
                # retain its exact JSON order despite sorted publication records.
                schema = engine._ordered_json_bytes(settings['output_schema'])
                metadata['settings']['output_schema_json'] = schema.decode('utf-8')
                metadata['settings']['output_schema_sha256'] = hashlib.sha256(schema).hexdigest()
            metadata['outcome'] = {**dict(counts), 'inputs': len(seen_inputs)}
            if isinstance(header.get('comparison'), dict):
                metadata['comparison'] = _scalars(header['comparison'],
                    {'missing_baseline', 'input_mismatch', 'not_comparable', 'unchanged', 'changed'})

        def result_rows():
            with spool.open(encoding='utf-8') as stream:
                for line in stream:
                    yield json.loads(line)

        return write_publication(output, metadata,
            {'inputs.jsonl': input_rows(), 'results.jsonl': result_rows()})
