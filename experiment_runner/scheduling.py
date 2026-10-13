"""Fair host-local request queue; workers start only after shared admission.

The existing private backoff state owns both the queue and provider tickets.
There is no daemon, per-experiment concurrency setting or second capacity pool.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time

from .provider_backoff import GLOBAL_REQUEST_LIMIT, SharedProviderBackoff, process_start

LIMIT = GLOBAL_REQUEST_LIMIT
_slot = ContextVar('experiment_request_slot', default=None)
_queue = ContextVar('experiment_queue', default=None)


def directory():
    return Path(os.environ.get('ORACLE_EXPERIMENT_BACKOFF_DIRECTORY') or
                Path(os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache')) /
                'oracle-mvp' / 'provider-backoff')


def current_slot():
    return _slot.get()


def _ready(run):
    return run.get('endpoint_ready', run['ready'])


def _endpoint_full(state, run):
    endpoint = run.get('endpoint_admission')
    if endpoint is None:
        return False
    active = [job.get('endpoint_admission') for job in state['queue']['running'].values()]
    matching = [value for value in active if value is not None and value['scope'] == endpoint['scope']]
    capacity = min([endpoint['capacity'], *(value['capacity'] for value in matching)])
    return len(matching) >= capacity


def _blocked(state, run, now):
    until = run['until']
    reason = 'provider_backoff' if until > now else None
    if run.get('scopes') is not None:
        for scope in SharedProviderBackoff._keys(run['scopes']).values():
            value = state['scopes'].get(scope)
            if value and value['until'] > now:
                until = max(until, value['until'])
                reason = 'transport_backoff' if scope.startswith('transport:') else 'account_backoff'
            elif value and value['limit'] and sum(scope in ticket['generations'] for ticket in state['tickets'].values()) >= value['limit']:
                reason = reason or 'recovery_limit'
    if _endpoint_full(state, run):
        reason = reason or 'capacity'
    return reason, until


def _queue_state(state):
    queue = state.setdefault('queue', {'experiments': {}, 'waiting': {}, 'running': {}, 'sequence': 0})
    if (not isinstance(queue, dict) or set(queue) != {'experiments', 'waiting', 'running', 'sequence'}
            or any(not isinstance(queue[key], dict) for key in ('experiments', 'waiting', 'running'))
            or type(queue['sequence']) is not int):
        raise ValueError('invalid experiment queue state')
    return queue


def _clean(state, now):
    queue = _queue_state(state)
    for owner, run in list(queue['experiments'].items()):
        if process_start(run['pid']) != run['process_start']:
            alive = [job for job in queue['running'].values() if job['owner'] == owner
                     and job.get('worker_pid') and job.get('worker_start') and job['expires'] > now
                     and process_start(job['worker_pid']) == job['worker_start']]
            if alive:
                run['pending'] = len(alive)
                run['ready'] = 0
                if 'endpoint_ready' in run:
                    run['endpoint_ready'] = 0
            else:
                del queue['experiments'][owner]
    for kind in ('waiting', 'running'):
        for key, job in list(queue[kind].items()):
            if (job['owner'] not in queue['experiments']
                    or kind == 'waiting' and process_start(queue['experiments'][job['owner']]['pid']) != queue['experiments'][job['owner']]['process_start']
                    or kind == 'running' and job['expires'] <= now):
                if kind == 'running' and job.get('provider_ticket'):
                    state['tickets'].pop(job['provider_ticket'], None)
                del queue[kind][key]
    for run in queue['experiments'].values():
        if 'endpoint_ready' in run:
            # Already-loaded older coordinators also read ready when choosing
            # a fair contender. Hide busy endpoints from that shared selection;
            # keep actual waiting counts for current status and observations.
            run['ready'] = 0 if _endpoint_full(state, run) else run['endpoint_ready']
    return queue


def _fill_slots(gate, state, now):
    """Any coordinator can reserve the next fair registered waiters' slots."""
    queue = state['queue']
    counts = {owner: 0 for owner in queue['experiments']}
    for job in queue['running'].values():
        counts[job['owner']] += 1
    while gate.occupancy(state, now) < LIMIT:
        eligible = {}
        heads = {job['owner']: identifier for identifier, job in queue['waiting'].items()}
        for owner, run in queue['experiments'].items():
            if not run.get('ready') or run.get('pause_reason'):
                continue
            identifier = heads.get(owner)
            job = queue['waiting'].get(identifier)
            blocked, until = _blocked(state, run, now)
            if job is not None:
                job['cooldown_until'] = until
            if blocked:
                if job is not None:
                    job['wait_reason'] = blocked
            else:
                eligible[owner] = identifier
        if not eligible:
            return
        owner = min(eligible, key=lambda value: (counts[value], queue['experiments'][value]['last_grant']))
        identifier = eligible[owner]
        if identifier is None:
            # A ready owner may be handing its local admission lock to its next
            # waiter. Preserve its fair turn while that waiter registers.
            return
        job, run = queue['waiting'][identifier], queue['experiments'][owner]
        timeout = job.get('timeout_seconds')
        if timeout is None:
            # Older coordinators only recognize admission performed by their
            # own polling thread. Preserve their fair turn until they claim it.
            return
        if type(timeout) is not int or not 1 <= timeout <= 3600:
            raise ValueError('invalid experiment waiter timeout')
        ticket = None
        if job['scopes'] is not None:
            result = gate._acquire(state, now, gate._keys(job['scopes']), LIMIT,
                                   timeout, run['pid'], run['process_start'])
            ticket = result.get('ticket')
            if ticket is None:
                job['wait_reason'] = result['wait_reason']
                return
        del queue['waiting'][identifier]
        queue['running'][identifier] = {**job, 'started': now,
            'expires': now + timeout + 30, 'provider_ticket': ticket}
        queue['sequence'] += 1
        run['last_grant'] = queue['sequence']
        run['endpoint_ready' if 'endpoint_ready' in run else 'ready'] -= 1
        counts[owner] += 1


def _snapshot(state, now, saved):
    """Capture counts and constraints from one coherent decoded state."""
    queue = _clean(state, now)
    occupied = SharedProviderBackoff.occupancy(state, now)
    epoch = time.time()
    experiments = {}
    for owner, run in queue['experiments'].items():
        running = [job for job in queue['running'].values() if job['owner'] == owner]
        waiting = [job for job in queue['waiting'].values() if job['owner'] == owner]
        blocked, until = _blocked(state, run, now)
        reason = blocked or next((job.get('wait_reason') for job in waiting
                                 if job.get('wait_reason')), run.get('wait_reason'))
        if reason == 'capacity' and occupied < LIMIT and not blocked:
            reason = 'ready'
        experiments[owner] = {
            'pid': run['pid'], 'queued': max(0, run['pending'] - len(running)),
            'running': len(running), 'ready': _ready(run), 'waiting': len(waiting),
            'eligible_ready': _ready(run) if not blocked and not run.get('pause_reason') else 0,
            'wait_reason': reason, 'pause_reason': run.get('pause_reason'),
            'cooldown_until_utc': (datetime.fromtimestamp(epoch + until - now, timezone.utc).isoformat()
                                   if until > now else None),
        }
    common = {
        'observed_at_utc': datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
        'monotonic_seconds': now, 'boot_id': state['boot'],
        'global_running': occupied, 'global_limit': LIMIT,
        'global_ready': sum(run['ready'] for run in experiments.values()),
        'global_eligible_ready': sum(run['eligible_ready'] for run in experiments.values()),
        'experiments': experiments,
    }
    run = queue['experiments'].get(saved['owner'])
    if run is None:
        return {**saved, **common,
                'state': 'interrupted' if saved['state'] == 'running' else saved['state'],
                'queued': 0, 'running': 0, 'ready': 0, 'eligible_ready': 0,
                'wait_reason': None, 'cooldown_until_utc': None}
    running = [j for j in queue['running'].values() if j['owner'] == saved['owner']]
    waiting = [j for j in queue['waiting'].values() if j['owner'] == saved['owner']]
    elapsed = [now - j['started'] for j in running]
    waits = [now - j['queued_at'] for j in waiting]
    reason = experiments[saved['owner']]['wait_reason']
    blocked, until = _blocked(state, run, now)
    if not blocked and reason == 'provider_backoff' and not waiting and run['until'] <= now:
        reason = None
    if reason is None and run['pending'] > len(running):
        reason = 'capacity' if occupied >= LIMIT else 'ready'
    cooldown = max([0, *(j.get('cooldown_until', 0) - now for j in waiting), until - now])
    orphaned = process_start(run['pid']) != run['process_start']
    return {**saved, **common,
            'state': 'interrupted' if orphaned else 'pausing' if run.get('pause_reason') else 'running',
            'pause_reason': run.get('pause_reason'), 'queued': max(0, run['pending'] - len(running)),
            'running': len(running), 'ready': _ready(run),
            'eligible_ready': experiments[saved['owner']]['eligible_ready'],
            'wait_reason': reason, 'queue_wait_seconds': max([saved.get('queue_wait_seconds', 0), *waits]),
            'running_elapsed_seconds': max([0, *elapsed]),
            'cooldown_until_utc': (datetime.fromtimestamp(epoch + cooldown, timezone.utc).isoformat()
                                   if cooldown else None),
            'cases': {j['case']: {'state': 'running', 'elapsed_seconds': now - j['started']}
                      for j in running} | {j['case']: {'state': 'queued', 'wait_reason': j.get('wait_reason'),
                                                     'wait_seconds': now - j['queued_at']} for j in waiting}}


def snapshot(run_directory):
    path = Path(run_directory) / 'scheduling.json'
    if not path.exists():
        return {'state': 'idle', 'queued': 0, 'running': 0, 'global_limit': LIMIT}
    saved = json.loads(path.read_text())
    gate = SharedProviderBackoff(directory())
    state, now = gate.observation()
    return _snapshot(state, now, saved)


def pause(run_directory, reason='requested'):
    saved = snapshot(run_directory)
    if saved['state'] in ('paused', 'finished'):
        return saved
    if saved['state'] not in ('running', 'pausing'):
        raise ValueError('no running experiment supports handover here; older runners must finish or be restarted separately')
    gate = SharedProviderBackoff(directory())
    with gate._state() as (state, now):
        queue = _clean(state, now)
        run = queue['experiments'].get(saved['owner'])
        if run is None:
            raise ValueError('experiment exited before pause was requested')
        run['pause_reason'] = reason
    return snapshot(run_directory)


class ExperimentQueue:
    def __init__(self, run_directory, count, *, scopes=None, scope_loader=None,
                 endpoint_loader=None, worker_identity=None):
        self.path = Path(run_directory) / 'scheduling.json'
        self.gate = SharedProviderBackoff(directory())
        self.owner = secrets.token_hex(16)
        self.scopes, self.scope_loader = scopes, scope_loader
        self.endpoint_admission, self.endpoint_loader = None, endpoint_loader
        self.scope_lock = threading.Lock()
        self.admission_lock = threading.Lock()
        self.scope_error = None
        self.worker_identity = worker_identity
        self.last_worker_identity = worker_identity
        self.worker_lock = threading.Lock()
        self.pause_reason = None
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.saved = {'owner': self.owner, 'state': 'running', 'queued': count, 'running': 0,
                      'queue_wait_seconds': 0, 'global_limit': LIMIT, 'valid': 0, 'failed': 0, 'reused': 0}
        self.last_progress = 0
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            queue['experiments'][self.owner] = {'pid': os.getpid(), 'process_start': process_start(os.getpid()),
                'pending': count, 'ready': 0, 'scopes': None, 'last_grant': 0, 'until': 0, 'wait_reason': None}
            observation = _snapshot(state, now, self.saved)
        self._save()
        self._record(observation, 'started')

    def _save(self):
        temporary = self.path.with_name('.scheduling-' + self.owner)
        temporary.write_text(json.dumps(self.saved) + '\n')
        os.replace(temporary, self.path)

    def _append_history(self, value, event):
        # The caller holds the experiment lock, never the host-wide state lock.
        observation = {key: item for key, item in value.items() if key != 'cases'}
        with self.path.with_suffix('.jsonl').open('a') as output:
            output.write(json.dumps({**observation, 'schema_version': 1, 'event': event}) + '\n')

    def _record(self, value, event):
        with self.lock:
            self._append_history(value, event)

    def progress(self, *, force=False):
        with self.lock:
            if not force and time.monotonic() - self.last_progress < 1:
                return
            self.last_progress = time.monotonic()
            value = snapshot(self.path.parent)
            self._append_history(value, 'progress')
            print(f"experiment-runner: queued={value['queued']} running={value['running']} "
                  f"valid={value['valid']} failed={value['failed']} reused={value['reused']} "
                  f"global={value.get('global_running', 0)}/{LIMIT} "
                  f"wait={value.get('wait_reason') or '-'}"
                  f" observed_at_utc={value['observed_at_utc']} ready={value['ready']}"
                  f" global_ready={value['global_ready']} global_eligible_ready={value['global_eligible_ready']}"
                  + (f" eligible={value['cooldown_until_utc']}" if value.get('cooldown_until_utc') else ''),
                  file=sys.stderr, flush=True)

    def completed(self, *, failed=False, reused=False, warmup=False):
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            queue['experiments'][self.owner]['pending'] -= 1
        with self.lock:
            if not warmup:
                self.saved['failed' if failed else 'valid'] += 1
                self.saved['reused'] += int(reused)
            self._save()

    def extend(self, count):
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            queue['experiments'][self.owner]['pending'] += count

    def warming(self, active):
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            queue['experiments'][self.owner]['wait_reason'] = 'prefix_warmup' if active else None
            observation = _snapshot(state, now, self.saved)
        self._record(observation, 'prefix_warmup_started' if active else 'prefix_warmup_finished')

    def check_pause(self):
        try:
            queue = _queue_state(self.gate.read_state())
            self.pause_reason = queue['experiments'][self.owner].get('pause_reason')
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            with self.gate._state() as (state, now):
                queue = _clean(state, now)
                self.pause_reason = queue['experiments'][self.owner].get('pause_reason')
        if self.pause_reason:
            self.stopped.set()
            from .extraction_pipeline import ExperimentPaused
            raise ExperimentPaused('experiment paused: ' + self.pause_reason)

    def _wait_hint(self, identifier, refresh_at):
        """Busy observations only defer polling; every grant stays under the lock."""
        if self.stopped.is_set() or time.monotonic() >= refresh_at:
            return False
        try:
            state = self.gate.read_state()
            queue = _queue_state(state)
            run = queue['experiments'][self.owner]
            if (run.get('pause_reason') or identifier in queue['running']
                    or identifier not in queue['waiting']):
                return False
            now = time.monotonic()
            running = queue['running'].values()
            if (any(job['expires'] <= now for job in running)
                    or any(ticket['expires'] <= now for ticket in state['tickets'].values())):
                return False
            # Deliberately conservative: no repeated process-liveness scans in
            # idle pollers. Periodic locked refresh removes stale reservations.
            tracked = {job['provider_ticket'] for job in running}
            occupied = len(queue['running']) + sum(key not in tracked for key in state['tickets'])
            if occupied >= LIMIT:
                return True
            blocked, _ = _blocked(state, run, now)
            return blocked is not None
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return False

    def check_worker(self, identity):
        self.check_pause()
        if self.worker_identity is not None:
            from . import compatibility
            with self.worker_lock:
                if not compatibility.compatible(self.worker_identity, identity):
                    pause(self.path.parent, 'runner_updated')
                    self.check_pause()
                elif identity['runner_sha256'] != self.last_worker_identity['runner_sha256']:
                    manifest = json.loads((self.path.parent / 'manifest.json').read_text())
                    compatibility.upgrade_worker(self.path.parent, manifest, identity)
                    self.last_worker_identity = identity

    def worker_started(self, identifier, pid):
        start = process_start(pid)
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            job = queue['running'][identifier]
            if start is not None:
                job.update(worker_pid=pid, worker_start=start)
            ticket = state['tickets'].get(job['provider_ticket'])
            if ticket is not None and start is not None:
                ticket.update(pid=pid, process_start=start)

    def cooldown(self, until):
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            run = queue['experiments'][self.owner]
            run['until'] = until
            run['wait_reason'] = 'provider_backoff' if until > now else None
            observation = _snapshot(state, now, self.saved)
        self._record(observation, 'backoff')

    @contextmanager
    def activate(self):
        token = _queue.set(self)
        try:
            yield self
        finally:
            _queue.reset(token)

    @contextmanager
    def slot(self, case, timeout_seconds):
        self.check_pause()
        # Scope discovery is one metadata-only call per batch, never inference.
        with self.scope_lock:
            if self.scope_error is not None:
                raise self.scope_error
            if self.scope_loader is not None:
                try:
                    self.scopes = self.scope_loader()
                except Exception as error:
                    self.scope_error = error
                    raise
                self.scope_loader = None
            if self.endpoint_loader is not None:
                try:
                    self.endpoint_admission = self.endpoint_loader()
                except Exception as error:
                    self.scope_error = error
                    raise
                self.endpoint_loader = None
        identifier = secrets.token_hex(16)
        queued_at = time.monotonic()
        token = None
        provider_ticket = None
        pending = False
        registered = False
        admitted = False
        ready_key = 'ready'
        try:
            with self.gate._state() as (state, now):
                queue = _clean(state, now)
                run = queue['experiments'][self.owner]
                run['scopes'] = self.scopes
                if self.endpoint_admission is not None:
                    run['endpoint_admission'] = self.endpoint_admission
                    run.setdefault('endpoint_ready', run['ready'])
                    ready_key = 'endpoint_ready'
                run[ready_key] += 1
                pending = True
                observation = _snapshot(state, now, self.saved)
            self._record(observation, 'ready')
            # One shared waiter per experiment; other pending requests wait
            # locally without repeatedly scanning or rewriting host state.
            with self.admission_lock:
                with self.gate._state() as (state, now):
                    queue = _clean(state, now)
                    waiter = {'owner': self.owner, 'case': case, 'queued_at': queued_at,
                                                    'scopes': self.scopes, 'wait_reason': 'capacity',
                                                    'timeout_seconds': timeout_seconds,
                                                    **({'endpoint_admission': self.endpoint_admission}
                                                       if self.endpoint_admission is not None else {})}
                    queue['waiting'][identifier] = waiter
                    registered = True
                refresh_at = 0
                while True:
                    if self._wait_hint(identifier, refresh_at):
                        self.progress()
                        time.sleep(0.1)
                        continue
                    admitted = False
                    with self.gate._state() as (state, now):
                        queue = _clean(state, now)
                        if self.stopped.is_set():
                            from .extraction_pipeline import NewJobLimitReached
                            raise NewJobLimitReached('experiment stopped before queued request dispatch')
                        self.pause_reason = queue['experiments'][self.owner].get('pause_reason')
                        if self.pause_reason:
                            self.stopped.set()
                            from .extraction_pipeline import ExperimentPaused
                            raise ExperimentPaused('experiment paused: ' + self.pause_reason)
                        if identifier not in queue['waiting'] and identifier not in queue['running']:
                            # An unadopted peer grant expired while this live
                            # coordinator was delayed. Register the same request
                            # again; no model attempt has been claimed or sent.
                            queue['waiting'][identifier] = dict(waiter)
                            queue['experiments'][self.owner][ready_key] += 1
                        _fill_slots(self.gate, state, now)
                        reservation = queue['running'].get(identifier)
                        if reservation is not None:
                            provider_ticket = reservation['provider_ticket']
                            pending = False
                            admitted = True
                            observation = _snapshot(state, now, self.saved)
                        else:
                            job = queue['waiting'][identifier]
                            blocked, _ = _blocked(state, queue['experiments'][self.owner], now)
                            if not blocked:
                                job['wait_reason'] = 'capacity' if self.gate.occupancy(state, now) >= LIMIT else 'fair_share'
                    if admitted:
                        break
                    refresh_at = time.monotonic() + 5 + int(self.owner[:8], 16) % 1000 / 1000
                    self.progress()
                    time.sleep(0.1)
            self._record(observation, 'admitted')
            with self.lock:
                self.saved['queue_wait_seconds'] = max(self.saved['queue_wait_seconds'], time.monotonic() - queued_at)
                self._save()
            self.check_pause()
            token = _slot.set({'provider_ticket': provider_ticket, 'identifier': identifier, 'queue': self})
            yield
        finally:
            if token is not None:
                _slot.reset(token)
            # Native workers report provider facts before returning. If launch
            # was canceled/refused, release the unused provider admission too.
            if provider_ticket:
                self.gate.report(provider_ticket, {})
            with self.gate._state() as (state, now):
                queue = _clean(state, now)
                reservation = queue['running'].get(identifier)
                if pending and (not registered or identifier in queue['waiting']):
                    queue['experiments'][self.owner][ready_key] -= 1
                if reservation is not None and reservation.get('provider_ticket'):
                    state['tickets'].pop(reservation['provider_ticket'], None)
                queue['waiting'].pop(identifier, None)
                queue['running'].pop(identifier, None)
                observation = _snapshot(state, now, self.saved)
            self._record(observation, 'released' if admitted else 'cancelled')

    def close(self):
        value = snapshot(self.path.parent)
        with self.gate._state() as (state, now):
            queue = _clean(state, now)
            queue['experiments'].pop(self.owner, None)
            _clean(state, now)
            observation = _snapshot(state, now, {**self.saved, 'state': 'finished'})
        with self.lock:
            self.saved.update(value, state='paused' if value.get('pause_reason') else 'finished', unresolved=value['queued'], queued=0,
                              running=0, wait_reason=None, cooldown_until_utc=None, cases={})
            self._save()
            self._append_history({**observation, 'state': self.saved['state']}, 'finished')
        self.progress(force=True)


def active_queue():
    return _queue.get()
