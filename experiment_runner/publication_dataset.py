"""Package explicitly selected JSON values once, with exact source restoration.

This module changes storage only. It preserves files, rows, trial identities and
the original publication manifests. Shared values are indexed on disk; neither
preparation nor verification accumulates corpus values in memory.
References use snapshot-local integer IDs; shared values retain full SHA-256.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import tempfile


FORMAT = 'publication-dataset-v1'
_DIGEST = re.compile(r'[0-9a-f]{64}')
_FILENAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]*\.jsonl')
_MAX_JSON = 16 * 1024 * 1024


def _require(condition, message):
    if not condition:
        raise ValueError('publication dataset: ' + message)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _path(name):
    _require(isinstance(name, str) and name and '\\' not in name
             and name == str(PurePosixPath(name)) and not name.startswith('/')
             and all(part not in ('', '.', '..') for part in name.split('/')),
             'paths must be portable source-relative names')
    return name


def _no_symlink(path):
    path = Path(path).absolute()
    _require(not any(parent.is_symlink() for parent in (path, *path.parents)),
             'symlinks are not supported')
    return path.resolve()


def _stamp(path):
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _inventory(root):
    files, directories = {}, set()
    for parent, children, names in os.walk(root, followlinks=False):
        parent = Path(parent)
        for name in children:
            path = parent/name
            _require(not path.is_symlink() and path.is_dir(), 'symlinks are not supported')
            directories.add(_path(path.relative_to(root).as_posix()))
        for name in names:
            path = parent/name
            _require(not path.is_symlink() and path.is_file(), 'only regular files are supported')
            files[_path(path.relative_to(root).as_posix())] = _stamp(path)
    return files, directories


def _rules(rules):
    _require(isinstance(rules, list), 'rules must be a list')
    result = []
    for value in rules:
        _require(isinstance(value, dict) and set(value) <= {'format', 'file', 'path', 'min_bytes'}
                 and {'format', 'file', 'path'} <= value.keys(), 'invalid selection rule')
        _require(isinstance(value['format'], str) and value['format']
                 and isinstance(value['file'], str) and _FILENAME.fullmatch(value['file'])
                 and isinstance(value['path'], list)
                 and all(isinstance(part, str) for part in value['path']), 'invalid selection rule')
        minimum = value.get('min_bytes', 256)
        _require(type(minimum) is int and minimum >= 0, 'min_bytes must be a nonnegative integer')
        normalized = {**value, 'min_bytes': minimum}
        for previous in result:
            if (previous['format'], previous['file']) == (value['format'], value['file']):
                a, b = previous['path'], value['path']
                _require(a[:len(b)] != b and b[:len(a)] != a, 'overlapping rule paths are not supported')
        result.append(normalized)
    return result


def _excluded(value):
    _require(isinstance(value, dict), 'excluded publications must be a map')
    result = {}
    for name, digest in value.items():
        name = _path(name)
        _require(isinstance(digest, str) and _DIGEST.fullmatch(digest), 'invalid excluded manifest hash')
        _require(not any(name.startswith(other+'/') or other.startswith(name+'/') for other in result),
                 'excluded publication roots overlap')
        result[name] = digest
    return result


def _omitted(name, excluded):
    return any(str(parent) in excluded for parent in PurePosixPath(name).parents)


def _hash_file(path):
    before = _stamp(path)
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
            size += len(block)
    _require(before == _stamp(path), 'file changed while reading')
    return {'sha256': digest.hexdigest(), 'bytes': size}


def _load(path):
    before = _stamp(path)
    _require(before[3] <= _MAX_JSON, 'JSON document exceeds the bounded reader limit')
    with path.open('rb') as stream:
        raw = stream.read(before[3] + 1)
    _require(before == _stamp(path) and len(raw) == before[3], 'JSON document changed while reading')
    try:
        return json.loads(raw)
    except (ValueError, RecursionError) as error:
        raise ValueError('publication dataset: invalid JSON document') from error


def _lines(path):
    with path.open('rb') as stream:
        while True:
            line = stream.readline(_MAX_JSON + 257)
            if not line:
                return
            _require(len(line) <= _MAX_JSON + 256, 'JSONL row exceeds the bounded reader limit')
            try:
                value = json.loads(line)
                _require(_canonical(value) + b'\n' == line, 'selected JSONL is not canonical UTF-8 JSON plus LF')
            except (ValueError, TypeError, RecursionError) as error:
                raise ValueError('publication dataset: selected JSONL is not canonical') from error
            yield line, value


def _marker(value):
    if isinstance(value, dict) and set(value) == {'$ref'}:
        reference = value['$ref']
        if type(reference) is int and reference > 0:
            _require(reference <= 2**63-1, 'shared reference ID is out of range')
            return reference
    return None


def _collision(value):
    _require(_marker(value) is None, 'original JSON contains a reserved shared-reference marker')
    if isinstance(value, dict):
        for child in value.values():
            _collision(child)
    elif isinstance(value, list):
        for child in value:
            _collision(child)


def _apply(value, path, replace):
    if not path:
        return replace(value)
    part, tail = path[0], path[1:]
    if part == '*' and isinstance(value, list):
        return [_apply(child, tail, replace) for child in value]
    if part != '*' and isinstance(value, dict) and part in value:
        value[part] = _apply(value[part], tail, replace)
    return value


@contextmanager
def _index(directory):
    connection = sqlite3.connect(str(directory/'index.sqlite'))
    try:
        connection.execute('PRAGMA journal_mode=OFF')
        connection.execute('PRAGMA cache_size=-8192')
        connection.execute('PRAGMA temp_store=FILE')
        connection.execute('CREATE TABLE values_index (id INTEGER PRIMARY KEY, hash TEXT UNIQUE NOT NULL, '
                           'value BLOB NOT NULL, refs INTEGER NOT NULL DEFAULT 0)')
        yield connection
    finally:
        connection.close()


@contextmanager
def _staging(source, output):
    source, output = _no_symlink(source), _no_symlink(output)
    _require(source.is_dir(), 'source must be a directory')
    _require(not output.is_relative_to(source) and not source.is_relative_to(output), 'destination overlaps source')
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        _require(not output.exists(), 'destination already exists')
        with tempfile.TemporaryDirectory(prefix='.publication-dataset-', dir=output.parent) as temporary:
            workspace = Path(temporary)
            staged = workspace/'payload'
            staged.mkdir()
            yield source, output, workspace, staged
            _require(not output.exists(), 'destination already exists')
            staged.rename(output)
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _context(root, parent, rules):
    path = root/parent/'experiment.json'
    if not path.is_file():
        return {}, {}
    try:
        metadata = _load(path)
    except ValueError:
        return {}, {}
    if not isinstance(metadata, dict):
        return {}, {}
    selected = {rule['file']: [] for rule in rules if rule['format'] == metadata.get('format')}
    for rule in rules:
        if rule['format'] == metadata.get('format'):
            selected[rule['file']].append(rule)
    declared = metadata.get('files', {})
    if isinstance(declared, dict):
        for name in selected.keys() & declared.keys():
            _require(_no_symlink(root/parent/name).is_file(), 'missing declared selected file: ' + name)
    return metadata, selected


def _source_binding(metadata, name, digest, size, count):
    records = metadata.get('files', {})
    bound = records.get(name) if isinstance(records, dict) else None
    _require(isinstance(bound, dict) and bound.get('sha256') == digest
             and bound.get('bytes') == size
             and ('records' not in bound or bound['records'] == count), 'original publication source binding differs')


def _snapshot(files, excluded):
    original = {name: {'sha256': value['source_sha256'], 'bytes': value['source_bytes']}
                for name, value in files.items()}
    return _sha(_canonical({'files': original, 'excluded': excluded}))


def prepare_dataset(source, output, *, rules, excluded=None):
    """Create a new storage package; source directories and bytes stay untouched."""
    rules, excluded = _rules(rules), _excluded({} if excluded is None else excluded)
    with _staging(Path(source), Path(output)) as (source, output, workspace, staged):
        original_files, original_directories = _inventory(source)
        _require('dataset.json' not in original_files and not any(
            name == 'shared' or name.startswith('shared/') for name in (*original_files, *original_directories)),
            'source uses reserved dataset names')
        for name, digest in excluded.items():
            path = source/name/'experiment.json'
            _require((source/name).is_dir() and path.is_file()
                     and _hash_file(path)['sha256'] == digest, 'excluded publication manifest binding differs')
        files, shared, previous_parent = {}, {}, None
        marker_overhead = len(_canonical({'$ref': 1})) - 1
        pool_overhead = len(_canonical({'id': 1, 'sha256': '0' * 64, 'value': None}) + b'\n') - 1 - len(b'null')
        with _index(workspace) as index:
            for name in sorted(original_files):
                if _omitted(name, excluded):
                    continue
                relative = PurePosixPath(name)
                if relative.parent != previous_parent:
                    metadata, selected = _context(source, relative.parent, rules)
                    previous_parent = relative.parent
                applicable = selected.get(relative.name, [])
                target = staged/name
                target.parent.mkdir(parents=True, exist_ok=True)
                before = _stamp(source/name)
                original_hash, original_bytes, count = hashlib.sha256(), 0, 0
                if applicable:
                    with target.open('wb') as stream:
                        for raw, value in _lines(source/name):
                            _require(len(raw) <= _MAX_JSON, 'source JSONL row exceeds the bounded reader limit')
                            _collision(value)
                            original_hash.update(raw)
                            original_bytes += len(raw)
                            count += 1
                            for rule in applicable:
                                def replace(child, minimum=rule['min_bytes']):
                                    body = _canonical(child)
                                    if len(body) < minimum or len(body) <= marker_overhead + 1:
                                        return child
                                    digest = _sha(body)
                                    old = index.execute('SELECT id,value FROM values_index WHERE hash=?', (digest,)).fetchone()
                                    _require(old is None or bytes(old[1]) == body, 'shared value hash collision')
                                    if old is None:
                                        identifier = index.execute('INSERT INTO values_index(hash,value) VALUES(?,?)',
                                                                   (digest, body)).lastrowid
                                    else:
                                        identifier = old[0]
                                    index.execute('UPDATE values_index SET refs=refs+1 WHERE id=?', (identifier,))
                                    return {'$ref': identifier}
                                value = _apply(value, rule['path'], replace)
                            stream.write(_canonical(value) + b'\n')
                    _source_binding(metadata, relative.name, original_hash.hexdigest(), original_bytes, count)
                else:
                    with (source/name).open('rb') as stream, target.open('wb') as destination:
                        for raw in iter(lambda: stream.read(1024 * 1024), b''):
                            destination.write(raw)
                            original_hash.update(raw)
                            original_bytes += len(raw)
                _require(before == _stamp(source/name), 'source changed while packaging')
                files[name] = {**_hash_file(target), 'source_sha256': original_hash.hexdigest(),
                               'source_bytes': original_bytes, 'transformed': bool(applicable)}
                if applicable:
                    files[name]['records'] = count
            # A shared record is worthwhile only when its repeated inline bytes
            # exceed all reference markers plus the complete shared JSONL row.
            index.execute('UPDATE values_index SET refs=0 WHERE refs<2 OR '
                          'refs*(length(value)-?-length(CAST(id AS TEXT)))<='
                          'length(value)+?+length(CAST(id AS TEXT))', (marker_overhead, pool_overhead))
            for name, record in files.items():
                if not record['transformed']:
                    continue
                temporary = workspace/'rewritten.jsonl'
                with temporary.open('wb') as stream:
                    for _, value in _lines(staged/name):
                        stream.write(_canonical(_restore_value(value, index, unshared_only=True)) + b'\n')
                temporary.replace(staged/name)
                record.update(_hash_file(staged/name))
            active, stream = None, None
            try:
                for identifier, digest, body in index.execute('SELECT id,hash,value FROM values_index '
                                                            'WHERE refs>0 ORDER BY (id % 256),id'):
                    shard = f'shared/{identifier % 256:02x}.jsonl'
                    if shard != active:
                        if stream:
                            stream.close()
                        (staged/'shared').mkdir(exist_ok=True)
                        stream = (staged/shard).open('wb')
                        active = shard
                    stream.write(b'{"id":' + str(identifier).encode() + b',"sha256":"' + digest.encode()
                                 + b'","value":' + bytes(body) + b'}\n')
                    shared.setdefault(shard, {'records': 0})['records'] += 1
            finally:
                if stream:
                    stream.close()
            for name in shared:
                shared[name].update(_hash_file(staged/name))
        _require((original_files, original_directories) == _inventory(source), 'source tree changed while packaging')
        manifest = dict(format=FORMAT, schema_version=1, rules=rules, excluded=excluded,
                        files=files, shared=shared, source_snapshot_sha256=_snapshot(files, excluded))
        with (staged/'dataset.json').open('wb') as stream:
            stream.write(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2,
                                    allow_nan=False).encode() + b'\n')
        verification = verify_dataset(staged)
        manifest_hash = _hash_file(staged/'dataset.json')['sha256']
    return dict(output=str(output), manifest_sha256=manifest_hash, **verification)


def _manifest(dataset, *, complete=True):
    _no_symlink(dataset/'dataset.json')
    manifest = _load(dataset/'dataset.json')
    _require(isinstance(manifest, dict) and set(manifest) == {'format', 'schema_version', 'rules',
        'excluded', 'files', 'shared', 'source_snapshot_sha256'} and manifest['format'] == FORMAT
        and type(manifest['schema_version']) is int and manifest['schema_version'] == 1,
        'unsupported dataset manifest')
    _rules(manifest['rules'])
    _excluded(manifest['excluded'])
    _require(isinstance(manifest['files'], dict) and isinstance(manifest['shared'], dict), 'invalid manifest file maps')
    for name, record in manifest['files'].items():
        _path(name)
        _require(name not in ('dataset.json', 'shared') and not name.startswith('shared/')
                 and not _omitted(name, manifest['excluded']) and isinstance(record, dict)
                 and set(record) <= {'sha256', 'bytes', 'source_sha256', 'source_bytes', 'transformed', 'records'}
                 and {'sha256', 'bytes', 'source_sha256', 'source_bytes', 'transformed'} <= record.keys()
                 and type(record['transformed']) is bool, 'invalid packaged file record')
        for key in ('sha256', 'source_sha256'):
            _require(isinstance(record[key], str) and _DIGEST.fullmatch(record[key]), 'invalid file hash')
        for key in ('bytes', 'source_bytes'):
            _require(type(record[key]) is int and record[key] >= 0, 'invalid file size')
        _require(('records' in record) == record['transformed']
                 and ('records' not in record or type(record['records']) is int and record['records'] >= 0),
                 'invalid file record count')
    for name, record in manifest['shared'].items():
        _require(re.fullmatch(r'shared/[0-9a-f]{2}\.jsonl', name) and isinstance(record, dict)
                 and set(record) == {'sha256', 'bytes', 'records'}
                 and isinstance(record['sha256'], str) and _DIGEST.fullmatch(record['sha256'])
                 and all(type(record[key]) is int and record[key] >= 0 for key in ('bytes', 'records')),
                 'invalid shared file record')
    _require(manifest['source_snapshot_sha256'] == _snapshot(manifest['files'], manifest['excluded']),
             'source snapshot binding differs')
    if complete:
        files, directories = _inventory(dataset)
        expected = {'dataset.json', *manifest['files'], *manifest['shared']}
        allowed_directories = {str(parent) for name in expected for parent in PurePosixPath(name).parents if str(parent) != '.'}
        _require(set(files) == expected and directories <= allowed_directories, 'extra or missing dataset files')
    return manifest


def _shared(dataset, manifest, index, names=None):
    for name in sorted(manifest['shared'] if names is None else names):
        _require(name in manifest['shared'], 'missing shared shard: ' + name)
        record = manifest['shared'][name]
        _require(_no_symlink(dataset/name).is_file(), 'missing shared shard: ' + name)
        _require(_hash_file(dataset/name) == {key: record[key] for key in ('sha256', 'bytes')}, 'shared file hash or size differs')
        count = 0
        for _, row in _lines(dataset/name):
            _require(isinstance(row, dict) and set(row) == {'id', 'sha256', 'value'}
                     and type(row['id']) is int and 0 < row['id'] <= 2**63-1
                     and isinstance(row['sha256'], str) and _DIGEST.fullmatch(row['sha256']), 'invalid shared value record')
            body, digest = _canonical(row['value']), row['sha256']
            _require(_sha(body) == digest, 'shared value hash differs')
            _require(name == f"shared/{row['id'] % 256:02x}.jsonl", 'shared value bucket differs')
            _collision(row['value'])
            try:
                index.execute('INSERT INTO values_index(id,hash,value) VALUES(?,?,?)', (row['id'], digest, body))
            except sqlite3.IntegrityError as error:
                raise ValueError('publication dataset: duplicate shared value') from error
            count += 1
        _require(count == record['records'], 'shared record count differs')


def _references(value, rules, index=None):
    allowed = {}
    for rule in rules:
        def selected(child):
            if _marker(child):
                allowed[id(child)] = rule['min_bytes']
            return child
        _apply(value, rule['path'], selected)

    def visit(child):
        identifier = _marker(child)
        if identifier:
            _require(id(child) in allowed, 'shared reference outside selection rule')
            if index is not None:
                row = index.execute('SELECT length(value) FROM values_index WHERE id=?', (identifier,)).fetchone()
                _require(row is not None, 'missing shared reference: ' + str(identifier))
                _require(row[0] >= allowed[id(child)], 'shared reference is below selection threshold')
            yield identifier
        elif isinstance(child, dict):
            for item in child.values():
                yield from visit(item)
        elif isinstance(child, list):
            for item in child:
                yield from visit(item)
    yield from visit(value)


def _restore_value(value, index, *, unshared_only=False):
    identifier = _marker(value)
    if identifier:
        row = index.execute('SELECT value,refs FROM values_index WHERE id=?', (identifier,)).fetchone()
        _require(row is not None, 'missing shared reference: ' + str(identifier))
        if unshared_only and row[1] > 0:
            return value
        if not unshared_only:
            index.execute('UPDATE values_index SET refs=refs+1 WHERE id=?', (identifier,))
        return json.loads(bytes(row[0]))
    if isinstance(value, dict):
        return {key: _restore_value(child, index, unshared_only=unshared_only) for key, child in value.items()}
    if isinstance(value, list):
        return [_restore_value(child, index, unshared_only=unshared_only) for child in value]
    return value


def _reconstruct(dataset, name, record, index, rules):
    if record['transformed']:
        for _, value in _lines(dataset/name):
            for _ in _references(value, rules, index):
                pass
            yield _canonical(_restore_value(value, index)) + b'\n'
    else:
        with (dataset/name).open('rb') as stream:
            yield from iter(lambda: stream.read(1024 * 1024), b'')


def _check_original(dataset, name, record, index, rules, sink=None):
    _require(_hash_file(dataset/name) == {key: record[key] for key in ('sha256', 'bytes')}, 'packaged file hash or size differs')
    digest, size, count = hashlib.sha256(), 0, 0
    for raw in _reconstruct(dataset, name, record, index, rules):
        digest.update(raw)
        size += len(raw)
        count += 1
        if sink:
            sink.write(raw)
    _require(digest.hexdigest() == record['source_sha256'] and size == record['source_bytes'], 'original reconstruction hash or size differs')
    if record['transformed']:
        _require(count == record.get('records'), 'original row count differs')
    return count


def _selected_files(dataset, manifest, names):
    previous_parent = None
    for name in sorted(names):
        _require(_no_symlink(dataset/name).is_file(), 'missing packaged file: ' + name)
        relative = PurePosixPath(name)
        if relative.parent != previous_parent:
            _no_symlink(dataset/relative.parent/'experiment.json')
            metadata, selected = _context(dataset, relative.parent, manifest['rules'])
            previous_parent = relative.parent
        record, rules = manifest['files'][name], selected.get(relative.name, [])
        _require(record['transformed'] == bool(rules), 'file selection differs from dataset rules')
        yield name, record, metadata, rules


def _check_files(dataset, manifest, names, index, staged=None, prefix=''):
    for name, record, metadata, rules in _selected_files(dataset, manifest, names):
        if staged is None:
            count = _check_original(dataset, name, record, index, rules)
        else:
            target = staged/name[len(prefix):]
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('wb') as stream:
                count = _check_original(dataset, name, record, index, rules, stream)
        if record['transformed']:
            _source_binding(metadata, PurePosixPath(name).name, record['source_sha256'], record['source_bytes'], count)
        else:
            _require(record['sha256'] == record['source_sha256'] and record['bytes'] == record['source_bytes'],
                     'copied source binding differs')


def verify_dataset(dataset):
    """Verify package files, shared values and all reconstructed source bytes."""
    dataset = _no_symlink(dataset)
    manifest = _manifest(dataset)
    before = _inventory(dataset)
    with tempfile.TemporaryDirectory(prefix='publication-dataset-verify-') as temporary, _index(Path(temporary)) as index:
        _shared(dataset, manifest, index)
        _check_files(dataset, manifest, manifest['files'], index)
        _require(index.execute('SELECT COUNT(*) FROM values_index WHERE refs=0').fetchone()[0] == 0, 'unreferenced shared values')
        values, references = index.execute('SELECT COUNT(*),COALESCE(SUM(refs),0) FROM values_index').fetchone()
    _require(before == _inventory(dataset), 'dataset changed during verification')
    return dict(files=len(manifest['files']), shared_values=values, references=references,
                source_bytes=sum(row['source_bytes'] for row in manifest['files'].values()),
                packaged_bytes=sum(row['bytes'] for row in manifest['files'].values()) +
                               sum(row['bytes'] for row in manifest['shared'].values()),
                excluded=len(manifest['excluded']), source_snapshot_sha256=manifest['source_snapshot_sha256'])


def restore_dataset(dataset, output, publication=None):
    """Restore all files, or one study with only its files and shared shards present."""
    dataset = _no_symlink(dataset)
    manifest = _manifest(dataset, complete=publication is None)
    if publication is not None:
        publication = _path(publication)
        _require(publication + '/experiment.json' in manifest['files'], 'publication is not included')
    prefix = publication + '/' if publication is not None else ''
    names = [name for name in manifest['files'] if name.startswith(prefix)]
    before = _inventory(dataset) if publication is None else None
    manifest_stamp = _stamp(dataset/'dataset.json')
    with _staging(dataset, Path(output)) as (_, output, workspace, staged), _index(workspace) as index:
        shards = None
        stamps = {}
        if publication is not None:
            index.execute('CREATE TABLE needed(id INTEGER PRIMARY KEY)')
            for name, record, _, rules in _selected_files(dataset, manifest, names):
                stamps[name] = _stamp(dataset/name)
                if record['transformed']:
                    _require(_hash_file(dataset/name) == {key: record[key] for key in ('sha256', 'bytes')},
                             'packaged file hash or size differs')
                    for _, value in _lines(dataset/name):
                        index.executemany('INSERT OR IGNORE INTO needed(id) VALUES(?)',
                                          ((identifier,) for identifier in _references(value, rules)))
            shards = [f'shared/{bucket:02x}.jsonl' for (bucket,) in
                      index.execute('SELECT DISTINCT id % 256 FROM needed')]
            for name in shards:
                _require(_no_symlink(dataset/name).is_file(), 'missing shared shard: ' + name)
                stamps[name] = _stamp(dataset/name)
        _shared(dataset, manifest, index, shards)
        _check_files(dataset, manifest, names, index, staged, prefix)
        if publication is None:
            _require(index.execute('SELECT COUNT(*) FROM values_index WHERE refs=0').fetchone()[0] == 0,
                     'unreferenced shared values')
            _require(before == _inventory(dataset), 'dataset changed during restoration')
        else:
            _require(all(_stamp(dataset/name) == stamp for name, stamp in stamps.items()),
                     'dataset changed during restoration')
        _require(manifest_stamp == _stamp(dataset/'dataset.json'), 'manifest changed during restoration')
    return dict(output=str(output), publication=publication, files=len(names),
                bytes=sum(manifest['files'][name]['source_bytes'] for name in names),
                source_snapshot_sha256=manifest['source_snapshot_sha256'])
