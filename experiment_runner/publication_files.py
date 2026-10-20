"""Write small manifests and streamed final results without touching sources.

Callers select public fields and bind their source identity. This module owns
only the destination files, not research-specific selection or interpretation.
It uses the standard library so offline GPU workers can share the same writer.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile


def _hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _identity(metadata):
    if (type(metadata.get('schema_version')) is not int or metadata['schema_version'] < 1
            or any(not isinstance(metadata.get(name), str) or not metadata[name]
                   for name in ('format', 'source_id'))):
        raise ValueError('publication metadata requires schema_version, format and source_id')
    return metadata['format'], metadata['source_id']


@contextmanager
def _destination(output):
    if output.is_symlink():
        raise ValueError('publication destination must not be a symlink')
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield output, descriptor
    finally:
        os.close(descriptor)


def write_publication(output, metadata, payloads):
    """Stage JSONL iterables, then publish their metadata and SHA-256 bindings.

    A destination can refresh only the same format and source identity and must
    contain exactly its managed files. An unchanged export preserves mtimes.
    Generators may fill small metadata counts or source hashes while consumed;
    metadata is serialized after all payloads finish. Existing files remain
    intact if generation fails. During refresh, experiment.json is replaced
    last so an interrupted update is detectable from its payload hashes.
    """
    identity = _identity(metadata)
    if (not payloads or any(not isinstance(name, str) or not re.fullmatch(
            r'[A-Za-z0-9][A-Za-z0-9._-]*\.jsonl', name) for name in payloads)):
        raise ValueError('publication payload names must be plain JSONL filenames')
    names = sorted(payloads)
    with _destination(Path(output)) as (output, descriptor):
        if output.exists():
            if (not output.is_dir()
                    or {p.name for p in output.iterdir()} != {*names, 'experiment.json'}):
                raise ValueError('publication destination contains unrelated files')
            for name in [*names, 'experiment.json']:
                path = output/name
                if path.is_symlink():
                    raise ValueError('publication destination contains a symlink')
                if not path.is_file():
                    raise ValueError('publication destination contains unrelated files')
            with (output/'experiment.json').open(encoding='utf-8') as stream:
                previous = json.load(stream)
            if _identity(previous) != identity:
                raise ValueError('publication destination belongs to a different source')
        with tempfile.TemporaryDirectory(prefix='.publication-', dir=output.parent) as temporary:
            staged = Path(temporary)
            files = {}
            for name in names:
                count = 0
                with (staged/name).open('w', encoding='utf-8', newline='\n') as stream:
                    for row in payloads[name]:
                        json.dump(row, stream, ensure_ascii=False, sort_keys=True,
                                  separators=(',', ':'), allow_nan=False)
                        stream.write('\n')
                        count += 1
                    stream.flush()
                    os.fsync(stream.fileno())
                files[name] = dict(sha256=_hash(staged/name),
                                   bytes=(staged/name).stat().st_size, records=count)
            if _identity(metadata) != identity:
                raise ValueError('publication source identity changed during export')
            with (staged/'experiment.json').open('w', encoding='utf-8', newline='\n') as stream:
                json.dump({**metadata, 'files': files}, stream, ensure_ascii=False,
                          sort_keys=True, indent=2, allow_nan=False)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            if not output.exists():
                staged.rename(output)
            else:
                for name in [*names, 'experiment.json']:
                    if _hash(output/name) != _hash(staged/name):
                        os.replace(staged/name, output/name)
            os.fsync(descriptor)
    return dict(output=str(output), files=files)
