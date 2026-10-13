#!/usr/bin/env python3
"""Experiment-only admission shared by model-worker processes on this host."""

from contextlib import contextmanager
import fcntl
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import sys
import tempfile
import time

GLOBAL_REQUEST_LIMIT = 30

try:
    from .extraction_pipeline import provider_cooldown_scope, _provider_retry_after_delay
except ImportError:
    from extraction_pipeline import provider_cooldown_scope, _provider_retry_after_delay


def process_start(pid):
    """A PID plus Linux start time identifies a live worker across PID reuse."""
    try:
        value = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return value[19] if value[0] not in {'Z', 'X'} else None
    except (OSError, IndexError):
        return None


class SharedProviderBackoff:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('experiment backoff directory must be private and owned by this user')
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()

    @staticmethod
    def _keys(scopes):
        if (not isinstance(scopes, dict) or set(scopes) != {'account', 'transport'}
                or any(not isinstance(value, str) or not re.fullmatch('[a-f0-9]{64}', value)
                       for value in scopes.values())):
            raise ValueError('invalid experiment backoff scopes')
        return {role: role + ':' + value for role, value in scopes.items()}

    @contextmanager
    def _state(self):
        descriptor = os.open(self.directory / 'state.lock',
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = self.directory / 'state.json'
            try:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            except FileNotFoundError:
                state = {'schema_version': 1, 'boot': self.boot, 'scopes': {}, 'tickets': {}}
            else:
                with os.fdopen(descriptor) as source:
                    info = os.fstat(source.fileno())
                    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                            or info.st_mode & 0o077 or info.st_size > 1_048_576):
                        raise ValueError('invalid experiment backoff state file')
                    try:
                        state = json.load(source)
                    except (ValueError, UnicodeError) as error:
                        raise ValueError('invalid experiment backoff state') from error
            if (not isinstance(state, dict) or state.get('schema_version') != 1
                    or not isinstance(state.get('scopes'), dict)
                    or not isinstance(state.get('tickets'), dict)
                    or len(state['scopes']) > 256 or len(state['tickets']) > 1024):
                raise ValueError('invalid experiment backoff state')
            if state.get('boot') != self.boot:
                state = {'schema_version': 1, 'boot': self.boot, 'scopes': {}, 'tickets': {}}
            previous = json.dumps(state, sort_keys=True, separators=(',', ':'), allow_nan=False)
            now = time.monotonic()
            for value in state['scopes'].values():
                if (not isinstance(value, dict) or set(value) != {
                        'seconds', 'until', 'generation', 'limit', 'successes', 'ceiling', 'updated'}
                        or any(type(value[name]) not in (int, float) or not math.isfinite(value[name])
                               or value[name] < 0 for name in value)
                        or value['seconds'] > 60 or value['limit'] > value['ceiling']):
                    raise ValueError('invalid experiment backoff scope state')
            for identifier, ticket in list(state['tickets'].items()):
                if (not isinstance(ticket, dict) or set(ticket) != {
                        'pid', 'process_start', 'expires', 'started', 'generations', 'limits', 'concurrency'}
                        or type(ticket['pid']) is not int
                        or type(ticket['concurrency']) is not int or not 1 <= ticket['concurrency'] <= 1024
                        or type(ticket['expires']) not in (int, float)
                        or not isinstance(ticket['generations'], dict)
                        or not isinstance(ticket['limits'], dict)):
                    raise ValueError('invalid experiment backoff ticket')
                if (ticket['expires'] <= now or process_start(ticket['pid']) != ticket['process_start']):
                    del state['tickets'][identifier]
            occupied = {key for ticket in state['tickets'].values() for key in ticket['generations']}
            for key, value in list(state['scopes'].items()):
                if key not in occupied and value['until'] <= now and value['updated'] + 3600 < now:
                    del state['scopes'][key]
            yield state, now
            if json.dumps(state, sort_keys=True, separators=(',', ':'), allow_nan=False) == previous:
                return
            descriptor, temporary = tempfile.mkstemp(prefix='.backoff-', dir=self.directory)
            try:
                with os.fdopen(descriptor, 'w') as output:
                    json.dump(state, output, separators=(',', ':'), allow_nan=False)
                    output.write('\n')
                os.replace(temporary, path)
            finally:
                Path(temporary).unlink(missing_ok=True)

    def try_acquire(self, scopes, *, concurrency, timeout_seconds, owner_pid):
        keys = self._keys(scopes)
        if (type(concurrency) is not int or not 1 <= concurrency <= 1024
                or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600
                or type(owner_pid) is not int or owner_pid < 1):
            raise ValueError('invalid experiment admission limits')
        start = process_start(owner_pid)
        if start is None:
            raise ValueError('experiment worker exited before admission')
        with self._state() as (state, now):
            return self._acquire(state, now, keys, concurrency, timeout_seconds, owner_pid, start)

    def _acquire(self, state, now, keys, concurrency, timeout_seconds, owner_pid, start):
        if self.occupancy(state, now) >= GLOBAL_REQUEST_LIMIT:
            return {'wait_seconds': 0.1, 'wait_reason': 'capacity', 'cooldown_seconds': 0}
        for key in keys.values():
            if key not in state['scopes']:
                if len(state['scopes']) >= 256:
                    raise ValueError('experiment backoff scope capacity reached')
                state['scopes'][key] = {'seconds': 0, 'until': 0, 'generation': 0,
                    'limit': 0, 'successes': 0, 'ceiling': concurrency, 'updated': now}
        waits = []
        for key in keys.values():
            value = state['scopes'][key]
            if value['until'] > now:
                waits.append(value['until'] - now)
            active = sum(key in ticket['generations'] for ticket in state['tickets'].values())
            if value['limit'] and active >= value['limit']:
                waits.append(0.1)
        if waits:
            cooling = [key for key in keys.values() if state['scopes'][key]['until'] > now]
            return {'wait_seconds': max(waits),
                    'wait_reason': ('transport_backoff' if any(key.startswith('transport:') for key in cooling)
                                    else 'account_backoff' if cooling else 'recovery_limit'),
                    'cooldown_seconds': max([0, *(state['scopes'][key]['until'] - now for key in cooling)])}
        if len(state['tickets']) >= 1024:
            raise ValueError('experiment backoff worker capacity reached')
        identifier = secrets.token_hex(16)
        state['tickets'][identifier] = {'pid': owner_pid, 'process_start': start,
            'expires': now + timeout_seconds + 30, 'started': now,
            'concurrency': concurrency,
            'generations': {key: state['scopes'][key]['generation'] for key in keys.values()},
            'limits': {key: state['scopes'][key]['limit'] for key in keys.values()}}
        for key in keys.values():
            state['scopes'][key]['ceiling'] = max(state['scopes'][key]['ceiling'], concurrency)
        return {'ticket': identifier}

    @staticmethod
    def occupancy(state, now):
        queue = state.get('queue', {})
        owners = queue.get('experiments', {})
        running = [job for job in queue.get('running', {}).values()
                   if job['owner'] in owners and job['expires'] > now
                   and (process_start(owners[job['owner']]['pid']) == owners[job['owner']]['process_start']
                        or job.get('worker_pid') and job.get('worker_start')
                        and process_start(job['worker_pid']) == job['worker_start'])]
        tracked = {job['provider_ticket'] for job in running}
        return len(running) + sum(ticket not in tracked for ticket in state['tickets'])

    def validate(self, identifier, scopes, owner_pid):
        keys = self._keys(scopes)
        with self._state() as (state, _):
            ticket = state['tickets'].get(identifier)
            valid = bool(ticket and set(ticket['generations']) == set(keys.values()) and process_start(owner_pid))
            if valid:
                ticket.update(pid=owner_pid, process_start=process_start(owner_pid))
                for job in state.get('queue', {}).get('running', {}).values():
                    if job['provider_ticket'] == identifier:
                        job.update(worker_pid=owner_pid, worker_start=ticket['process_start'])
            return {'valid': valid}

    def acquire(self, scopes, *, concurrency, timeout_seconds, owner_pid):
        while True:
            admitted = self.try_acquire(scopes, concurrency=concurrency,
                timeout_seconds=timeout_seconds, owner_pid=owner_pid)
            if 'ticket' in admitted:
                return admitted
            # Short wakeups observe another process's extended wait and exit;
            # never hold a file lock during a request or a cooldown.
            time.sleep(min(0.25, admitted['wait_seconds']))

    def report(self, identifier, record):
        if not isinstance(identifier, str) or not re.fullmatch('[a-f0-9]{32}', identifier):
            raise ValueError('invalid experiment admission ticket')
        result = record.get('model_result') or {}
        metadata = result.get('response_metadata') or {}
        proxy = metadata.get('proxy_status') or {}
        role = provider_cooldown_scope(record)
        completed = (not metadata.get('abort_reason') and
            (metadata.get('terminal_event') == 'response.completed'
             or isinstance(proxy, dict) and (proxy.get('terminal_event') == 'completed'
                                             or proxy.get('state') == 'complete')
             or result.get('stop_reason') == 'stop'))
        with self._state() as (state, now):
            ticket = state['tickets'].pop(identifier, None)
            if ticket is None:
                return {'reported': False}
            for key, generation in ticket['generations'].items():
                value = state['scopes'][key]
                if role and key.startswith(role + ':'):
                    if generation == value['generation']:
                        value['seconds'] = min(60, max(2, value['seconds'] * 2))
                        value['generation'] += 1
                        # A previous larger run must not set this run's recovery
                        # ceiling after its workers have drained.
                        value['ceiling'] = max([ticket['concurrency'], *(other['concurrency']
                            for other in state['tickets'].values() if key in other['generations'])])
                        value['until'] = max(value['until'], now + max(
                            value['seconds'], _provider_retry_after_delay(record) or 0))
                        value['limit'], value['successes'] = 1, 0
                    elif any(metadata.get(key) is not None for key in ('retry_after', 'retry_after_ms')):
                        # Older requests belong to an already counted outage.
                        # Only an explicit server deadline can extend its wait;
                        # they cannot undo capacity recovered by newer requests.
                        delay = _provider_retry_after_delay(record)
                        if delay is not None:
                            value['until'] = max(value['until'], now + delay)
                    value['updated'] = now
                elif (completed and generation == value['generation']
                        and ticket['started'] >= value['until']
                        and ticket['limits'][key] == value['limit'] and value['limit']):
                    value['successes'] += 1
                    if value['successes'] >= value['limit']:
                        if value['limit'] >= value['ceiling']:
                            value['seconds'], value['until'], value['limit'] = 0, 0, 0
                        else:
                            value['limit'] = min(value['ceiling'], value['limit'] * 2)
                        value['successes'] = 0
                    value['updated'] = now
            return {'reported': True}


def main():
    # Only the experiment runner supplies this directory and enables this CLI.
    directory = os.environ.get('ORACLE_EXPERIMENT_BACKOFF_DIRECTORY')
    if not directory:
        base = Path(os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache'))
        directory = base / 'oracle-mvp' / 'provider-backoff'
    request = json.loads(sys.stdin.read(16384))
    gate = SharedProviderBackoff(directory)
    if request.get('action') == 'acquire':
        response = gate.acquire(request['scopes'], concurrency=request['concurrency'],
            timeout_seconds=request['timeout_seconds'], owner_pid=request['owner_pid'])
    elif request.get('action') == 'report':
        response = gate.report(request['ticket'], request['record'])
    elif request.get('action') == 'validate':
        response = gate.validate(request['ticket'], request['scopes'], request['owner_pid'])
    else:
        raise ValueError('invalid experiment backoff operation')
    print(json.dumps(response))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError):
        sys.stderr.write('Experiment admission state unavailable; no new request admitted.\n')
        sys.exit(1)
