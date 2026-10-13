"""Request compatibility and append-only worker provenance for saved runs."""
import hashlib
import json
from pathlib import Path


OPERATIONAL_FILES = frozenset({
    '__init__.py', 'cli.py', 'experiment.py', 'extraction_pipeline.py',
    'provider_backoff.py', 'scheduling.py', 'compatibility.py',
})


def request_code(identity):
    registry = Path(__file__).with_name('frontier_runner') / 'request-compatibility.json'
    known = json.loads(registry.read_text())
    value = identity.get('request_code_sha256')
    if value is not None:
        if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('invalid request code SHA-256')
        # Exact, reviewed observation-only revisions share a request family.
        # Unknown fingerprints still compare strictly; raw receipts stay intact.
        return known.get(value, value)
    return known.get(identity.get('runner_sha256'))


def compatible(original, current):
    if current.get('runner_version') != original.get('runner_version'):
        return False
    previous, following = request_code(original), request_code(current)
    if previous is not None and following is not None and previous != following:
        return False
    return (current.get('runner_sha256') == original.get('runner_sha256')
            or previous is not None and previous == following)


def worker_history(directory, manifest):
    """Verify transitions against the immutable manifest and prior receipt prefix."""
    directory = Path(directory)
    path = directory / 'worker-upgrades.jsonl'
    original = {k: manifest[k] for k in ('runner_version', 'runner_sha256')}
    if 'request_code_sha256' in manifest:
        original['request_code_sha256'] = manifest['request_code_sha256']
    identities = [original]
    if not path.exists():
        return identities
    results = (directory / 'results.jsonl').read_bytes() if (directory / 'results.jsonl').exists() else b''
    digest = hashlib.sha256((directory / 'manifest.json').read_bytes()).hexdigest()
    last_size = 0
    for line in path.read_text().splitlines():
        row = json.loads(line)
        size = row['results_prefix_bytes']
        if (row['manifest_sha256'] != digest or row['from'] != identities[-1]
                or type(size) is not int or not last_size <= size <= len(results)
                or hashlib.sha256(results[:size]).hexdigest() != row['results_prefix_sha256']
                or not compatible(original, row['to'])):
            raise ValueError('invalid worker upgrade history')
        identities.append(row['to'])
        last_size = size
    return identities


def upgrade_worker(directory, manifest, identity):
    if not compatible(manifest, identity):
        raise ValueError('model runner request-producing code changed; prepare a new experiment')
    identities = worker_history(directory, manifest)
    current = {k: identity[k] for k in ('runner_version', 'runner_sha256')}
    if 'request_code_sha256' in identity:
        current['request_code_sha256'] = identity['request_code_sha256']
    if current['runner_sha256'] == identities[-1]['runner_sha256']:
        return
    directory = Path(directory)
    results = (directory / 'results.jsonl').read_bytes() if (directory / 'results.jsonl').exists() else b''
    row = dict(manifest_sha256=hashlib.sha256((directory / 'manifest.json').read_bytes()).hexdigest(),
               **{'from': identities[-1], 'to': current},
               reason='compatible request-producing code; runner provenance update',
               results_prefix_bytes=len(results), results_prefix_sha256=hashlib.sha256(results).hexdigest())
    with (directory / 'worker-upgrades.jsonl').open('a') as stream:
        stream.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')


def receipt_identity(directory, manifest, record, *, identities=None):
    for identity in identities if identities is not None else worker_history(directory, manifest):
        if all(record.get(k) == identity[k] for k in ('runner_version', 'runner_sha256')):
            if record.get('request_code_sha256') is not None and request_code(record) != request_code(identity):
                raise ValueError('saved receipt request code differs')
            return identity
    raise ValueError('saved receipt worker has no audited upgrade')
