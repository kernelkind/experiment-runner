#!/usr/bin/env python3
"""Preserve extraction workflows and audited batches using the generic model runner."""

from __future__ import annotations

import argparse
import difflib
import importlib.util
import fcntl
import hashlib
import heapq
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence


if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from experiment_runner import project
from experiment_runner import compatibility

SCHEMA_VERSION = 1
PIPELINE_SCHEMA_VERSION = 2
REUSE_PIPELINE_SCHEMA_VERSION = 3
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNNER = Path(__file__).with_name("run-model.sh")
RESULTS_WRITE_LOCK = threading.Lock()
RUNTIME_FIELDS = {"provider", "timeout_seconds", "max_output_tokens"}
TAKEAWAY_ENVELOPE_VERSION = "xml-post-v1"
TAKEAWAY_INPUT_NORMALIZATION_VERSION = "remove-long-ascii-nip19-identifiers-v2"
URL_FREE_MASK_PROMPT_VERSIONS = frozenset({"MPv3.4"})
URL_SCHEME_PATTERN = re.compile(r"(?:https?|wss?)://", re.IGNORECASE)
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]*$")
NIP19_IDENTIFIER = re.compile(
    r"(?<![A-Za-z0-9_])"
    r"(?ai:(?:nostr:)?(?:nprofile|nevent|nrelay|naddr|npub|nsec|note)1"
    r"[023456789acdefghjklmnpqrstuvwxyz]{20,})"
    r"(?:[?#][^\s<>\"']*)?(?=$|[^A-Za-z0-9_])"
)

TAKEAWAY_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "takeaways": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        }
    },
    "required": ["takeaways"],
    "additionalProperties": False,
}
SUBJECT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "subjects": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        }
    },
    "required": ["subjects"],
    "additionalProperties": False,
}
MASK_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "fragments": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        }
    },
    "required": ["fragments"],
    "additionalProperties": False,
}
PROMINENCE_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {"prominence": {"type": "integer", "minimum": 0, "maximum": 100}},
    "required": ["prominence"],
    "additionalProperties": False,
}


class RunnerError(Exception):
    """A user-actionable wrapper failure."""


class ExperimentAdmissionUnavailable(RunnerError):
    """Admission failed locally before a model request could be sent."""


class ExperimentPaused(RunnerError):
    """Stop admission, preserve unfinished cases and drain active requests."""


class NewJobLimitReached(Exception):
    """The caller-requested limit on newly spawned model jobs was reached."""


class PromptSpendBudget:
    """Reserve a caller-verified maximum per request; never truncate responses."""

    def __init__(self, limit_usd, request_usd, records, *, prior_request_usd=None, reconciled_costs=None):
        self.limit = self.micros(limit_usd, ROUND_FLOOR)
        self.reservation = self.micros(request_usd, ROUND_CEILING)
        if self.limit <= 0 or self.reservation <= 0:
            raise RunnerError('positive dollar limits required')
        self.spent = self.pending = 0
        self.halted = False
        self.condition = threading.Condition()
        prior_reservation = max(self.reservation, self.micros(
            request_usd if prior_request_usd is None else prior_request_usd))
        for record in records:
            if record.get('execution') == 'cross_run_reuse':
                continue
            charge = self.charge(record, reconciled_costs)
            self.spent += prior_reservation if charge is None else charge
            self.halted |= charge is not None and charge > prior_reservation

    @staticmethod
    def micros(value, rounding=ROUND_CEILING):
        if isinstance(value, bool):
            raise RunnerError('invalid dollar amount')
        try:
            amount = Decimal(str(value))
            if not amount.is_finite() or amount < 0:
                raise ValueError
            return int((amount * 1000000).to_integral_value(rounding=rounding))
        except (ValueError, ArithmeticError):
            raise RunnerError('invalid dollar amount') from None

    @classmethod
    def charge(cls, record, reconciled_costs=None):
        try:
            return cls.micros(provider_cost(record, reconciled_costs))
        except (KeyError, TypeError, RunnerError):
            return None

    def claim(self):
        with self.condition:
            while self.spent + self.pending + self.reservation > self.limit:
                if self.halted or not self.pending:
                    raise NewJobLimitReached('dollar allowance exhausted')
                self.condition.wait()
            if self.halted:
                raise NewJobLimitReached('per-request bound exceeded')
            self.pending += self.reservation

    def settle(self, record):
        charge = self.charge(record)
        with self.condition:
            self.pending -= self.reservation
            self.spent += self.reservation if charge is None else charge
            self.halted |= charge is not None and charge > self.reservation
            self.condition.notify_all()


def provider_cost(record, reconciled_costs=None):
    native = record.get('model_result') or {}
    for key in ('response_metadata', 'usage'):
        native = native.get(key) if isinstance(native, dict) else None
    cost = native.get('cost') if isinstance(native, dict) else None
    if type(cost) in (int, float) and math.isfinite(cost) and cost >= 0:
        return cost
    return (reconciled_costs or {}).get(_sha256_bytes(_canonical_json_bytes(record)))


def load_reconciled_costs(run_directory, records):
    path = run_directory / 'cost-receipts.jsonl'
    if not path.exists():
        return {}
    by_hash = {_sha256_bytes(_canonical_json_bytes(r)): r for r in records}
    costs = {}
    for line in path.read_bytes().splitlines():
        receipt = json.loads(line)
        key = receipt.get('record_sha256')
        record = by_hash.get(key)
        generation = receipt.get('generation') or {}
        expected_id = ((record or {}).get('model_result') or {}).get('response_metadata', {}).get('id')
        cost = generation.get('total_cost')
        if (record is None or record.get('provider') != 'openrouter' or
            record.get('execution') == 'cross_run_reuse' or not expected_id or
            generation.get('id') != expected_id or receipt.get('source') != 'openrouter/generation' or
            type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0 or key in costs):
            raise RunnerError('invalid or mismatched cost receipt')
        native = provider_cost(record)
        if native is not None and native != cost:
            raise RunnerError('cost receipt conflicts with original billing')
        costs[key] = cost
    return costs


@dataclass
class RetryDirective:
    job_key: str
    reason: str
    matched: bool = False


@dataclass
class PersistentAttemptBudget:
    """An irreversible launch counter with an optional UTC admission deadline."""
    path: Path
    additional_paths: tuple[Path, ...] = ()

    @staticmethod
    def _read_value(path):
        value = _expect_object(_load_json(path, 'attempt budget'), 'attempt budget')
        expected = {'schema_version', 'limit', 'used'}
        if 'deadline_utc' in value:
            expected.add('deadline_utc')
        _expect_fields(value, 'attempt budget', expected)
        if (type(value['schema_version']) is not int or value['schema_version'] != 1
                or type(value['limit']) is not int or type(value['used']) is not int
                or value['limit'] < 1 or not 0 <= value['used'] <= value['limit']):
            raise RunnerError('invalid persistent attempt budget')
        deadline = None
        if 'deadline_utc' in value:
            raw = value['deadline_utc']
            if not isinstance(raw, str) or not re.fullmatch(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
                    r'(?:\.[0-9]+)?(?:Z|\+00:00)', raw):
                raise RunnerError('invalid persistent attempt budget deadline_utc')
            try:
                deadline = datetime.fromisoformat(raw).timestamp()
            except (ValueError, OverflowError, OSError) as error:
                raise RunnerError('invalid persistent attempt budget deadline_utc') from error
        return value, deadline

    def initialize(self, limit):
        """Create only the primary counter, or validate it without changing it.

        Initialization takes the same lock as claims. Existing usage and an
        optional deadline are preserved byte-for-byte; a different limit or
        malformed existing counter fails rather than replacing the allowance.
        Existing claims also prevent recreating or rolling back spent allowance.
        This neither reserves a launch nor initializes additional_paths.
        """
        if type(limit) is not int or limit < 1:
            raise RunnerError('attempt budget limit must be a positive integer')
        path = Path(self.path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_suffix(path.suffix + '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            claims = path.with_suffix(path.suffix + '.claims.jsonl')
            if path.exists():
                value, _ = self._read_value(path)
                if value['limit'] != limit:
                    raise RunnerError('existing attempt budget limit differs')
                if claims.exists():
                    try:
                        with claims.open('rb') as records:
                            for line in records:
                                claim = json.loads(line)
                                ordinal = claim['ordinal']
                                if type(ordinal) is not int or ordinal < 1:
                                    raise RunnerError('invalid attempt budget claims ledger')
                                if ordinal > value['used']:
                                    raise RunnerError('attempt budget rollback differs from existing claims')
                    except (OSError, ValueError, KeyError, TypeError) as error:
                        raise RunnerError('invalid attempt budget claims ledger') from error
            else:
                if claims.exists():
                    raise RunnerError('missing attempt budget has existing claims; cannot reset allowance')
                _atomic_write(path, _canonical_json_bytes(dict(schema_version=1, limit=limit, used=0)))

    @contextmanager
    def _values(self):
        paths = sorted({Path(path).resolve() for path in (self.path, *self.additional_paths)})
        with ExitStack() as locks:
            for path in paths:
                lock = locks.enter_context(path.with_suffix(path.suffix + '.lock').open('a'))
                fcntl.flock(lock, fcntl.LOCK_EX)
            values = []
            deadlines = []
            for path in paths:
                value, deadline = self._read_value(path)
                if deadline is not None:
                    deadlines.append(deadline)
                values.append((path, value))
            # Validate every locked counter before considering exhaustion.
            yield values, min(deadlines, default=None)

    def remaining(self) -> int:
        with self._values() as (values, deadline):
            if deadline is not None and time.time() >= deadline:
                return 0
            return min(value['limit'] - value['used'] for _, value in values)

    def claim(self, job_key: str | None = None) -> None:
        with self._values() as (values, deadline):
            if ((deadline is not None and time.time() >= deadline)
                    or any(value['used'] >= value['limit'] for _, value in values)):
                raise NewJobLimitReached
            for path, value in values:
                value['used'] += 1
                # All reservations precede transport. Interrupted writes can
                # consume allowance, but never launch an unreserved call.
                _atomic_write(path, _canonical_json_bytes(value))
                _append_json_line(path.with_suffix(path.suffix + '.claims.jsonl'),
                                  {'ordinal': value['used'], 'job_key': job_key})


@dataclass
class NewJobBudget:
    limit: int | None
    used: int = 0
    completed_notes: int = 0
    completed_takeaways: int = 0
    lock: Any = field(default_factory=threading.Lock, repr=False)
    spend: PromptSpendBudget | None = None
    persistent: PersistentAttemptBudget | None = None

    @property
    def exhausted(self) -> bool:
        return self.limit is not None and self.used >= self.limit

    def remaining(self) -> float:
        with self.lock:
            return min(float('inf') if self.limit is None else self.limit - self.used,
                       self.persistent.remaining() if self.persistent is not None else float('inf'))

    def claim(self, job_key: str | None = None) -> None:
        with self.lock:
            if self.limit is not None and self.used >= self.limit:
                raise NewJobLimitReached
            if self.persistent is not None:
                self.persistent.claim(job_key)
            if self.spend is not None:
                self.spend.claim()
            self.used += 1


@dataclass(frozen=True)
class PipelineRunOutcome:
    complete: bool
    new_jobs: int
    completed_notes: int
    completed_takeaways: int
    output: dict[str, object] | None


@dataclass(frozen=True)
class SubjectRunOutcome:
    complete: bool
    new_jobs: int
    completed_notes: int
    output: dict[str, object] | None


@dataclass(frozen=True)
class RunnerProvenance:
    initial_wrapper_sha256: str
    active_wrapper_sha256: str
    boundaries: tuple[tuple[int, str], ...]


@dataclass(frozen=True)
class PreparedManifest:
    manifest_sha256: str
    provenance: RunnerProvenance
    pending_wrapper_sha256: str | None
    pending_reason: str | None


@dataclass(frozen=True)
class ReuseSourceConfig:
    input_path: Path
    input_sha256: str
    run_directory: Path
    manifest_sha256: str
    results_sha256: str
    runner_upgrades_sha256: str | None
    output_sha256: str

    @classmethod
    def load(cls, raw: object, base: Path) -> "ReuseSourceConfig":
        value = _expect_object(raw, "reuse_source")
        _expect_fields(
            value,
            "reuse_source",
            {
                "input_path",
                "input_sha256",
                "run_directory",
                "manifest_sha256",
                "results_sha256",
                "runner_upgrades_sha256",
                "output_sha256",
            },
        )
        upgrades = value["runner_upgrades_sha256"]
        if upgrades is not None:
            upgrades = _expect_sha256(upgrades, "reuse_source.runner_upgrades_sha256")
        return cls(
            input_path=_resolve_path(
                value["input_path"], "reuse_source.input_path", base
            ),
            input_sha256=_expect_sha256(
                value["input_sha256"], "reuse_source.input_sha256"
            ),
            run_directory=_resolve_path(
                value["run_directory"], "reuse_source.run_directory", base
            ),
            manifest_sha256=_expect_sha256(
                value["manifest_sha256"], "reuse_source.manifest_sha256"
            ),
            results_sha256=_expect_sha256(
                value["results_sha256"], "reuse_source.results_sha256"
            ),
            runner_upgrades_sha256=upgrades,
            output_sha256=_expect_sha256(
                value["output_sha256"], "reuse_source.output_sha256"
            ),
        )

    def manifest_value(self) -> dict[str, object]:
        return {
            "input_path": str(self.input_path),
            "input_sha256": self.input_sha256,
            "run_directory": str(self.run_directory),
            "manifest_sha256": self.manifest_sha256,
            "results_sha256": self.results_sha256,
            "runner_upgrades_sha256": self.runner_upgrades_sha256,
            "output_sha256": self.output_sha256,
        }


@dataclass(frozen=True)
class SourceResultReference:
    line: int
    sha256: str
    attempt: int


@dataclass(frozen=True)
class SourceStageContract:
    prompt_version: str
    prompt: str
    prompt_sha256: str
    model: str
    reasoning_effort: str
    response_schema_sha256: str


@dataclass(frozen=True)
class ReusableJob:
    note_text: str
    rendered_prompt_sha256: str
    response: list[str] | int
    reference: SourceResultReference


@dataclass(frozen=True)
class LoadedReuseSource:
    config: ReuseSourceConfig
    stages: dict[str, SourceStageContract]
    jobs: dict[str, ReusableJob]


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _ordered_json_bytes(value: object) -> bytes:
    """Preserve property order for caller-supplied structured output schemas."""
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _expect_object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RunnerError(f"`{name}` must be an object")
    return value


def _expect_fields(
    value: dict[str, Any],
    name: str,
    expected: set[str],
) -> None:
    missing = sorted(expected - set(value))
    unexpected = sorted(set(value) - expected)
    if missing:
        raise RunnerError(f"`{name}.{missing[0]}` is required")
    if unexpected:
        raise RunnerError(f"`{name}.{unexpected[0]}` is not supported")


def _expect_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise RunnerError(f"`{name}` must be a nonempty string")
    if "\x00" in value:
        raise RunnerError(f"`{name}` must not contain a NUL byte")
    return value


def _expect_identifier(value: object, name: str) -> str:
    identifier = _expect_string(value, name)
    if not IDENTIFIER.fullmatch(identifier):
        raise RunnerError(f"`{name}` contains unsupported characters")
    return identifier


def _expect_sha256(value: object, name: str) -> str:
    digest = _expect_string(value, name)
    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise RunnerError(f"`{name}` must be a lowercase SHA-256 digest")
    return digest


def _resolve_path(value: object, name: str, base: Path) -> Path:
    raw = Path(_expect_string(value, name)).expanduser()
    if not raw.is_absolute():
        raw = base / raw
    return raw.resolve()


def _load_json(path: Path, name: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise RunnerError(f"`{name}` does not exist: {path}") from error
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RunnerError(f"could not read `{name}` at {path}: {error}") from error


def _atomic_write(path: Path, payload: bytes | Iterable[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as destination:
            if isinstance(payload, bytes):
                destination.write(payload)
            else:
                for chunk in payload:
                    destination.write(chunk)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_write_json(path: Path, value: object, *, sort_keys=False) -> None:
    """Preserve the JSON byte contract without materializing the whole document."""
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=sort_keys, separators=(",", ":"))

    def chunks():
        for chunk in encoder.iterencode(value):
            yield chunk.encode('utf-8')
        yield b'\n'

    _atomic_write(path, chunks())


def _append_json_line(path: Path, value: object, *, receipt_files=None) -> None:
    payload = _canonical_json_bytes(value)
    with RESULTS_WRITE_LOCK, path.open("ab") as destination:
        before = receipt_files._stamp(path) if receipt_files is not None else None
        offset = destination.tell()
        destination.write(payload)
        destination.flush()
        os.fsync(destination.fileno())
        if receipt_files is not None:
            receipt_files.appended(path, payload, offset, before, receipt_files._stamp(path))


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_auth_file(path: Path | None) -> None:
    if path is None:
        return
    if _is_within(path, REPOSITORY_ROOT) or (project.root() is not None and _is_within(path, project.root())):
        raise RunnerError(
            "auth_file must be outside the repository so its "
            "credentials cannot enter version control"
        )
    if not path.is_file():
        raise RunnerError(f"auth_file is missing or not a regular file: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode != 0o600:
        raise RunnerError(f"auth_file must have mode 0600, not {mode:04o}")


def _runner_environment() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(("CODEX_", "OPENAI_", "CHATGPT_"))
        and key not in {"ORACLE_EXPERIMENT_PROVIDER_TICKET", "ORACLE_EXPERIMENT_ADMISSION_MARKER",
                        "ORACLE_EXPERIMENT_CONCURRENCY", "ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256"}
    }


def _runtime_options(value: dict[str, Any], base: Path) -> dict[str, Any]:
    provider = _expect_identifier(value.get("provider"), "config.provider")
    if provider not in {"openai-codex", "openrouter", "local"}:
        raise RunnerError("provider must be openai-codex, openrouter or local")
    auth = value.get("auth_file")
    auth_file = None if auth is None else _resolve_path(auth, "auth_file", base)
    if provider == "local" and auth_file is not None:
        raise RunnerError("local model requests do not use credentials")
    if provider == "openrouter" and auth_file is not None:
        raise RunnerError("OpenRouter uses OPENROUTER_API_KEY, not auth_file")
    timeout = value.get("timeout_seconds", 600)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= 3600
    ):
        raise RunnerError("timeout_seconds must be an integer from 1 to 3600")
    cap = value.get("max_output_tokens")
    if provider in {"openrouter", "local"}:
        if cap is not None and (isinstance(cap, bool) or not isinstance(cap, int) or cap < 1):
            raise RunnerError("max_output_tokens must be positive when supplied")
    elif cap is not None:
        raise RunnerError("subscription transport does not enforce max_output_tokens")
    return {
        "provider": provider,
        "auth_file": auth_file,
        "timeout_seconds": timeout,
        "max_output_tokens": cap,
    }


def _resolve_executable(value: object, name: str, base: Path) -> Path:
    raw = _expect_string(value, name)
    if os.sep in raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = base / path
        path = path.resolve()
    else:
        found = shutil.which(raw)
        if found is None:
            raise RunnerError(f"could not find `{name}` executable `{raw}`")
        path = Path(found).resolve()
    try:
        mode = path.stat().st_mode
    except OSError as error:
        raise RunnerError(f"could not inspect `{name}` executable {path}: {error}")
    if not stat.S_ISREG(mode) or not os.access(path, os.X_OK):
        raise RunnerError(f"`{name}` is not an executable regular file: {path}")
    return path


@dataclass(frozen=True)
class StageConfig:
    prompt_registry_path: Path
    prompt_version: str
    prompt: str
    prompt_registry_sha256: str
    prompt_sha256: str
    model: str
    reasoning_effort: str

    @classmethod
    def load(
        cls,
        raw: object,
        name: str,
        base: Path,
    ) -> "StageConfig":
        value = _expect_object(raw, name)
        _expect_fields(
            value,
            name,
            {
                "prompt_registry_path",
                "prompt_version",
                "model",
                "reasoning_effort",
            },
        )
        registry_path = _resolve_path(
            value["prompt_registry_path"],
            f"{name}.prompt_registry_path",
            base,
        )
        registry = _expect_object(
            _load_json(registry_path, f"{name}.prompt_registry_path"),
            f"{name} prompt registry",
        )
        version = _expect_string(value["prompt_version"], f"{name}.prompt_version")
        if version not in registry:
            raise RunnerError(
                f"`{name}.prompt_version` `{version}` is absent from {registry_path}"
            )
        prompt = _expect_string(registry[version], f"{name} prompt `{version}`")
        if name == "mask":
            for placeholder in ("{POST}", "{TAKEAWAY}"):
                if placeholder not in prompt:
                    raise RunnerError(
                        f"mask prompt `{version}` is missing placeholder `{placeholder}`"
                    )
        return cls(
            prompt_registry_path=registry_path,
            prompt_version=version,
            prompt=prompt,
            prompt_registry_sha256=_sha256_file(registry_path),
            prompt_sha256=_sha256_bytes(prompt.encode("utf-8")),
            model=_expect_identifier(value["model"], f"{name}.model"),
            reasoning_effort=_expect_identifier(
                value["reasoning_effort"], f"{name}.reasoning_effort"
            ),
        )


@dataclass(frozen=True)
class RunnerConfig:
    schema_version: int
    config_path: Path
    runner_binary: Path
    auth_file: Path | None
    provider: str
    timeout_seconds: float
    max_output_tokens: int | None
    input_path: Path
    run_directory: Path
    takeaway: StageConfig
    prominence: StageConfig | None
    mask: StageConfig
    reuse_source: ReuseSourceConfig | None

    @classmethod
    def load(cls, path: Path) -> "RunnerConfig":
        config_path = path.expanduser().resolve()
        base = config_path.parent
        value = _expect_object(_load_json(config_path, "config"), "config")
        raw_schema_version = value.get("schema_version")
        if isinstance(raw_schema_version, bool) or not isinstance(
            raw_schema_version, int
        ):
            raise RunnerError("unsupported pipeline config schema version")
        if raw_schema_version == SCHEMA_VERSION:
            schema_version = SCHEMA_VERSION
            expected_fields = {
                "schema_version",
                "runner_binary",
                "auth_file",
                "input_path",
                "run_directory",
                "takeaway",
                "mask",
            }
        elif raw_schema_version == PIPELINE_SCHEMA_VERSION:
            schema_version = PIPELINE_SCHEMA_VERSION
            expected_fields = {
                "schema_version",
                "runner_binary",
                "auth_file",
                "input_path",
                "run_directory",
                "takeaway",
                "prominence",
                "mask",
            }
        elif raw_schema_version == REUSE_PIPELINE_SCHEMA_VERSION:
            schema_version = REUSE_PIPELINE_SCHEMA_VERSION
            expected_fields = {
                "schema_version",
                "runner_binary",
                "auth_file",
                "input_path",
                "run_directory",
                "takeaway",
                "prominence",
                "mask",
                "reuse_source",
            }
        else:
            raise RunnerError("unsupported pipeline config schema version")
        _expect_fields(
            value, "config", expected_fields | (RUNTIME_FIELDS & value.keys())
        )
        config = cls(
            schema_version=schema_version,
            config_path=config_path,
            runner_binary=_resolve_executable(
                value["runner_binary"], "runner_binary", base
            ),
            **_runtime_options(value, base),
            input_path=_resolve_path(value["input_path"], "input_path", base),
            run_directory=_resolve_path(value["run_directory"], "run_directory", base),
            takeaway=StageConfig.load(value["takeaway"], "takeaway", base),
            prominence=(
                None
                if schema_version == SCHEMA_VERSION
                else StageConfig.load(value["prominence"], "prominence", base)
            ),
            mask=StageConfig.load(value["mask"], "mask", base),
            reuse_source=(
                None
                if schema_version != REUSE_PIPELINE_SCHEMA_VERSION
                else ReuseSourceConfig.load(value["reuse_source"], base)
            ),
        )
        if (
            config.reuse_source is not None
            and config.run_directory == config.reuse_source.run_directory
        ):
            raise RunnerError(
                "reuse_source.run_directory must differ from run_directory"
            )
        return config


@dataclass(frozen=True)
class SubjectRunnerConfig:
    config_path: Path
    runner_binary: Path
    auth_file: Path | None
    provider: str
    timeout_seconds: float
    max_output_tokens: int | None
    input_path: Path
    run_directory: Path
    subject: StageConfig

    @classmethod
    def load(cls, path: Path) -> "SubjectRunnerConfig":
        config_path = path.expanduser().resolve()
        base = config_path.parent
        value = _expect_object(_load_json(config_path, "config"), "config")
        _expect_fields(
            value,
            "config",
            {
                "schema_version",
                "runner_binary",
                "auth_file",
                "input_path",
                "run_directory",
                "subject",
            }
            | (RUNTIME_FIELDS & value.keys()),
        )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != SCHEMA_VERSION
        ):
            raise RunnerError("unsupported subject config schema version")
        config = cls(
            config_path=config_path,
            runner_binary=_resolve_executable(
                value["runner_binary"], "runner_binary", base
            ),
            **_runtime_options(value, base),
            input_path=_resolve_path(value["input_path"], "input_path", base),
            run_directory=_resolve_path(value["run_directory"], "run_directory", base),
            subject=StageConfig.load(value["subject"], "subject", base),
        )
        return config


@dataclass(frozen=True)
class Note:
    note_id: str
    text: str


@dataclass(frozen=True)
class PromptJob:
    prompt_id: str
    text: str


@dataclass(frozen=True)
class PromptRunnerConfig:
    config_path: Path
    runner_binary: Path
    auth_file: Path | None
    provider: str
    timeout_seconds: float
    max_output_tokens: int | None
    run_directory: Path
    model: str
    reasoning_effort: str
    prompts: tuple[PromptJob, ...]
    output_schema: dict[str, object] | None = None
    concurrency: int = 30
    transport: str = "sse"
    prompt_cache_key: str | None = None
    prompt_cache_prefix: str | None = None

    @classmethod
    def load(cls, path: Path, *, files=None) -> "PromptRunnerConfig":
        config_path = path.expanduser().resolve()
        base = config_path.parent
        value = _expect_object(files.json(config_path) if files is not None
                               else _load_json(config_path, "config"), "config")
        _expect_fields(
            value,
            "config",
            {
                "schema_version",
                "runner_binary",
                "auth_file",
                "run_directory",
                "model",
                "reasoning_effort",
                "prompts",
            }
            | (({"output_schema", "concurrency", "transport", "prompt_cache_key", "prompt_cache_prefix"} | RUNTIME_FIELDS) & value.keys()),
        )
        if value["schema_version"] != SCHEMA_VERSION:
            raise RunnerError("unsupported config schema version")
        concurrency = value.get("concurrency", cls.concurrency)
        if type(concurrency) is not int or concurrency < 1:
            raise RunnerError("concurrency must be a positive integer")
        transport = value.get("transport", "http" if value.get("provider") == "local" else "sse")
        if transport not in ("sse", "websocket", "http"):
            raise RunnerError("transport must be sse, websocket or http")
        if (transport == "http") != (value.get("provider") == "local"):
            raise RunnerError("local model requests require http transport")
        if transport == "websocket" and value.get("provider") != "openai-codex":
            raise RunnerError("WebSocket is supported only for subscription Responses")
        output_schema = None
        if "output_schema" in value:
            output_schema = _expect_object(
                value["output_schema"], "config.output_schema"
            )
            if output_schema.get("type") != "object":
                raise RunnerError("`config.output_schema.type` must be `object`")
        raw_prompts = value["prompts"]
        if not isinstance(raw_prompts, list) or not raw_prompts:
            raise RunnerError("`config.prompts` must be a nonempty array")
        prompts: list[PromptJob] = []
        seen_ids: set[str] = set()
        for index, raw_prompt in enumerate(raw_prompts):
            name = f"config.prompts[{index}]"
            prompt = _expect_object(raw_prompt, name)
            _expect_fields(prompt, name, {"prompt_id", "text"})
            prompt_id = _expect_identifier(prompt["prompt_id"], f"{name}.prompt_id")
            if prompt_id in seen_ids:
                raise RunnerError(f"duplicate prompt_id `{prompt_id}`")
            seen_ids.add(prompt_id)
            prompts.append(
                PromptJob(
                    prompt_id=prompt_id,
                    text=_expect_string(prompt["text"], f"{name}.text"),
                )
            )
        config = cls(
            config_path=config_path,
            runner_binary=_resolve_executable(
                value["runner_binary"], "runner_binary", base
            ),
            **_runtime_options(value, base),
            run_directory=_resolve_path(value["run_directory"], "run_directory", base),
            model=_expect_identifier(value["model"], "config.model"),
            reasoning_effort=_expect_identifier(
                value["reasoning_effort"], "config.reasoning_effort"
            ),
            prompts=tuple(prompts),
            output_schema=output_schema,
            concurrency=concurrency,
            transport=transport,
            prompt_cache_key=value.get("prompt_cache_key"),
            prompt_cache_prefix=value.get("prompt_cache_prefix"),
        )
        if "prompt_cache_key" in value:
            _validate_prompt_cache_key(config.prompt_cache_key, config.provider, config.transport)
        if "prompt_cache_prefix" in value:
            prefix = config.prompt_cache_prefix
            if (config.provider != "openai-codex" or config.model != "gpt-6.1-sol"
                    or config.prompt_cache_key is None
                    or not isinstance(prefix, str) or not prefix.strip()
                    or any(not p.text.startswith(prefix) for p in config.prompts)):
                raise RunnerError("prompt_cache_prefix requires an exact nonempty prefix, GPT-6.1 Sol subscription and prompt_cache_key")
        return config


def _validate_prompt_cache_key(key, provider, transport):
    if provider != "openai-codex" or transport != "sse":
        raise RunnerError("prompt_cache_key requires subscription Responses over SSE")
    if (not isinstance(key, str) or not 1 <= len(key) <= 64
            or any(not 0x21 <= ord(c) <= 0x7e for c in key)):
        raise RunnerError("prompt_cache_key must contain 1–64 visible ASCII characters without spaces")


def _load_notes(path: Path) -> tuple[Note, ...]:
    value = _expect_object(_load_json(path, "input_path"), "input")
    _expect_fields(value, "input", {"schema_version", "notes"})
    if value["schema_version"] != SCHEMA_VERSION:
        raise RunnerError("unsupported input schema version")
    raw_notes = value["notes"]
    if not isinstance(raw_notes, list) or not raw_notes:
        raise RunnerError("`input.notes` must be a nonempty array")
    notes: list[Note] = []
    seen: set[str] = set()
    for index, raw_note in enumerate(raw_notes):
        name = f"input.notes[{index}]"
        note = _expect_object(raw_note, name)
        _expect_fields(note, name, {"note_id", "text"})
        note_id = _expect_string(note["note_id"], f"{name}.note_id")
        text = _expect_string(note["text"], f"{name}.text")
        if note_id in seen:
            raise RunnerError(f"duplicate note_id `{note_id}`")
        seen.add(note_id)
        notes.append(Note(note_id=note_id, text=text))
    return tuple(notes)


def _runner_identity(
    config: RunnerConfig | SubjectRunnerConfig | PromptRunnerConfig,
) -> dict[str, Any]:
    completed = subprocess.run(
        [str(config.runner_binary), "--identity"],
        check=False,
        capture_output=True,
        text=True,
        env=_runner_environment(),
        timeout=30,
    )
    if completed.returncode != 0:
        raise RunnerError(
            f"model runner --identity exited with status {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    try:
        identity = _expect_object(json.loads(completed.stdout), "runner identity")
    except json.JSONDecodeError as error:
        raise RunnerError("model runner returned invalid identity JSON") from error
    _expect_string(identity.get("runner_version"), "runner_version")
    _expect_sha256(identity.get("runner_sha256"), "runner_sha256")
    if 'request_code_sha256' in identity:
        _expect_sha256(identity['request_code_sha256'], 'request_code_sha256')
    if 'endpoint_admission' in identity and type(identity['endpoint_admission']) is not bool:
        raise RunnerError('runner endpoint_admission capability must be a boolean')
    return identity


def _runner_version(config: Any) -> str:
    return _runner_identity(config)["runner_version"]


def _runtime_manifest(config: Any) -> dict[str, Any]:
    identity = _runner_identity(config)
    return {
        "provider": config.provider,
        "runner_sha256": identity["runner_sha256"],
        **({'request_code_sha256': identity['request_code_sha256']}
           if 'request_code_sha256' in identity else {}),
        "timeout_seconds": config.timeout_seconds,
        "max_output_tokens": config.max_output_tokens,
    }


def _stage_manifest(
    stage: StageConfig,
    response_schema: dict[str, object],
) -> dict[str, object]:
    schema_bytes = _canonical_json_bytes(response_schema)
    return {
        "prompt_registry_path": str(stage.prompt_registry_path),
        "prompt_registry_sha256": stage.prompt_registry_sha256,
        "prompt_version": stage.prompt_version,
        "prompt": stage.prompt,
        "prompt_sha256": stage.prompt_sha256,
        "model": stage.model,
        "reasoning_effort": stage.reasoning_effort,
        "response_schema": response_schema,
        "response_schema_sha256": _sha256_bytes(schema_bytes),
    }


def _isolation_manifest() -> dict[str, object]:
    return {
        "fresh_process_per_job": True,
        "fresh_context_per_job": True,
        "system_instructions": "",
        "tools": [],
        "history": [],
        "transport": "pi-ai",
    }


def _build_manifest(
    config: RunnerConfig,
    runner_version: str,
) -> dict[str, object]:
    manifest: dict[str, object] = {
        "schema_version": config.schema_version,
        **_runtime_manifest(config),
        "wrapper_sha256": _sha256_file(Path(__file__).resolve()),
        "runner_binary": str(config.runner_binary),
        "runner_version": runner_version,
        "auth_file": None if config.auth_file is None else str(config.auth_file),
        "input_path": str(config.input_path),
        "input_sha256": _sha256_file(config.input_path),
        "takeaway_envelope_version": TAKEAWAY_ENVELOPE_VERSION,
        "takeaway_input_normalization_version": (TAKEAWAY_INPUT_NORMALIZATION_VERSION),
        "takeaway": _stage_manifest(config.takeaway, TAKEAWAY_SCHEMA),
        "mask": _stage_manifest(config.mask, MASK_SCHEMA),
        "isolation": _isolation_manifest(),
    }
    if config.prominence is not None:
        manifest["prominence"] = _stage_manifest(
            config.prominence,
            PROMINENCE_SCHEMA,
        )
        manifest["prominence_envelope_version"] = "xml-complete-post-takeaway-v1"
        manifest["prominence_input_normalization_version"] = (
            TAKEAWAY_INPUT_NORMALIZATION_VERSION
        )
    if config.reuse_source is not None:
        manifest["reuse_source"] = config.reuse_source.manifest_value()
    return manifest


def _build_prompt_manifest(
    config: PromptRunnerConfig,
    runner_version: str,
) -> dict[str, object]:
    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "mode": "prompt",
        **_runtime_manifest(config),
        "wrapper_sha256": _sha256_file(Path(__file__).resolve()),
        "config_path": str(config.config_path),
        "config_sha256": _sha256_file(config.config_path),
        "runner_binary": str(config.runner_binary),
        "runner_version": runner_version,
        "auth_file": None if config.auth_file is None else str(config.auth_file),
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "concurrency": config.concurrency,
        "prompts": [
            {"prompt_id": prompt.prompt_id, "text": prompt.text}
            for prompt in config.prompts
        ],
        "isolation": _isolation_manifest(),
    }
    if config.output_schema is not None:
        manifest["response_schema"] = config.output_schema
        manifest["response_schema_sha256"] = _sha256_bytes(
            _ordered_json_bytes(config.output_schema)
        )
    if config.transport != "sse":
        manifest["transport"] = config.transport
    if config.prompt_cache_key is not None:
        manifest["prompt_cache_key"] = config.prompt_cache_key
    if config.prompt_cache_prefix is not None:
        manifest["prompt_cache_prefix"] = config.prompt_cache_prefix
    return manifest


def _build_subject_manifest(
    config: SubjectRunnerConfig,
    runner_version: str,
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "mode": "atomic_subject_generation",
        **_runtime_manifest(config),
        "wrapper_sha256": _sha256_file(Path(__file__).resolve()),
        "runner_binary": str(config.runner_binary),
        "runner_version": runner_version,
        "auth_file": None if config.auth_file is None else str(config.auth_file),
        "input_path": str(config.input_path),
        "input_sha256": _sha256_file(config.input_path),
        "subject_envelope_version": TAKEAWAY_ENVELOPE_VERSION,
        "subject_input_normalization_version": (TAKEAWAY_INPUT_NORMALIZATION_VERSION),
        "subject": _stage_manifest(config.subject, SUBJECT_SCHEMA),
        "isolation": _isolation_manifest(),
    }


def _load_runner_upgrades(
    run_directory: Path,
    *,
    manifest_sha256: str,
    initial_wrapper_sha256: str,
) -> RunnerProvenance:
    path = run_directory / "runner-upgrades.jsonl"
    if not path.exists():
        return RunnerProvenance(initial_wrapper_sha256, initial_wrapper_sha256, ())
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise RunnerError(
            f"could not read runner upgrade ledger {path}: {error}"
        ) from error
    current_wrapper = initial_wrapper_sha256
    previous_first_result_line = 0
    result_size, _, result_line_count, _ = compatibility._results_prefix(run_directory)
    boundaries: list[tuple[int, str]] = []
    for line_number, line in enumerate(lines, start=1):
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as error:
            raise RunnerError(
                f"runner upgrade ledger line {line_number} is not valid JSON: {error}"
            ) from error
        record = _expect_object(raw, f"runner upgrade ledger line {line_number}")
        _expect_fields(
            record,
            f"runner upgrade ledger line {line_number}",
            {
                "schema_version",
                "manifest_sha256",
                "from_wrapper_sha256",
                "to_wrapper_sha256",
                "first_result_line",
                "results_prefix_bytes",
                "results_prefix_sha256",
                "reason",
            },
        )
        if record["schema_version"] != 2:
            raise RunnerError(
                f"runner upgrade ledger line {line_number} has unsupported schema"
            )
        if record["manifest_sha256"] != manifest_sha256:
            raise RunnerError(
                f"runner upgrade ledger line {line_number} belongs to another manifest"
            )
        from_wrapper = _expect_sha256(
            record["from_wrapper_sha256"],
            f"runner upgrade ledger line {line_number}.from_wrapper_sha256",
        )
        to_wrapper = _expect_sha256(
            record["to_wrapper_sha256"],
            f"runner upgrade ledger line {line_number}.to_wrapper_sha256",
        )
        if from_wrapper != current_wrapper or to_wrapper == from_wrapper:
            raise RunnerError(
                f"runner upgrade ledger line {line_number} does not continue its chain"
            )
        first_result_line = record["first_result_line"]
        prefix_bytes = record["results_prefix_bytes"]
        if (
            isinstance(first_result_line, bool)
            or not isinstance(first_result_line, int)
            or first_result_line < 1
            or first_result_line < previous_first_result_line
            or first_result_line > result_line_count + 1
            or isinstance(prefix_bytes, bool)
            or not isinstance(prefix_bytes, int)
            or prefix_bytes < 0
            or prefix_bytes > result_size
        ):
            raise RunnerError(
                f"runner upgrade ledger line {line_number} has invalid first_result_line"
            )
        _, prefix_sha256, prefix_line_count, prefix_terminated = compatibility._results_prefix(
            run_directory, prefix_bytes)
        if (
            _expect_sha256(
                record["results_prefix_sha256"],
                f"runner upgrade ledger line {line_number}.results_prefix_sha256",
            )
            != prefix_sha256
            or prefix_line_count != first_result_line - 1
            or (prefix_bytes and not prefix_terminated)
        ):
            raise RunnerError(
                f"runner upgrade ledger line {line_number} does not match its results prefix"
            )
        _expect_string(
            record["reason"], f"runner upgrade ledger line {line_number}.reason"
        )
        current_wrapper = to_wrapper
        previous_first_result_line = first_result_line
        boundaries.append((first_result_line, to_wrapper))
    return RunnerProvenance(
        initial_wrapper_sha256,
        current_wrapper,
        tuple(boundaries),
    )


def _prepare_manifest_directory(
    run_directory: Path,
    manifest: dict[str, object],
    *,
    runner_upgrade_reason: str | None = None,
) -> PreparedManifest:
    manifest_path = run_directory / "manifest.json"
    if run_directory.exists() and not run_directory.is_dir():
        raise RunnerError(f"run_directory is not a directory: {run_directory}")
    if run_directory.exists() and not manifest_path.exists():
        try:
            next(run_directory.iterdir())
        except StopIteration:
            pass
        else:
            raise RunnerError(
                "run_directory is nonempty but has no manifest; refusing to "
                "adopt unknown artifacts"
            )
    run_directory.mkdir(parents=True, exist_ok=True)
    manifest_bytes = _canonical_json_bytes(manifest)
    if manifest_path.exists():
        existing = _expect_object(
            _load_json(manifest_path, "run manifest"), "run manifest"
        )
        existing_bytes = _canonical_json_bytes(existing)
        manifest_sha256 = _sha256_bytes(existing_bytes)
        existing_without_wrapper = dict(existing)
        proposed_without_wrapper = dict(manifest)
        existing_wrapper = _expect_sha256(
            existing_without_wrapper.pop("wrapper_sha256", None),
            "run manifest.wrapper_sha256",
        )
        proposed_wrapper = _expect_sha256(
            proposed_without_wrapper.pop("wrapper_sha256", None),
            "proposed run manifest.wrapper_sha256",
        )
        if compatibility.compatible(existing, manifest):
            for key in ('runner_sha256', 'request_code_sha256'):
                proposed_without_wrapper.pop(key, None)
                existing_without_wrapper.pop(key, None)
        if existing_without_wrapper != proposed_without_wrapper:
            raise RunnerError(
                "run manifest does not match this configuration; use a new "
                "run_directory for different inputs, prompts, models, or effort"
            )
        provenance = _load_runner_upgrades(
            run_directory,
            manifest_sha256=manifest_sha256,
            initial_wrapper_sha256=existing_wrapper,
        )
        pending_wrapper: str | None = None
        pending_reason: str | None = None
        if provenance.active_wrapper_sha256 != proposed_wrapper:
            if runner_upgrade_reason is None:
                if compatibility.request_code(existing) is not None and compatibility.compatible(existing, manifest):
                    runner_upgrade_reason = 'compatible runner workflow update; request identity preserved'
                else:
                    raise RunnerError(
                        "run was created by another wrapper version; inspect the code "
                        "change and use --runner-upgrade-reason to append an audited "
                        "runner transition"
                    )
            pending_wrapper = proposed_wrapper
            pending_reason = runner_upgrade_reason
        elif runner_upgrade_reason is not None:
            raise RunnerError(
                "--runner-upgrade-reason was provided, but this run already uses "
                "the current wrapper"
            )
        manifest_bytes = existing_bytes
    else:
        if runner_upgrade_reason is not None:
            raise RunnerError(
                "--runner-upgrade-reason requires an existing run from another wrapper"
            )
        _atomic_write(manifest_path, manifest_bytes)
        manifest_sha256 = _sha256_bytes(manifest_bytes)
        proposed_wrapper = _expect_sha256(
            manifest["wrapper_sha256"], "proposed run manifest.wrapper_sha256"
        )
        provenance = RunnerProvenance(proposed_wrapper, proposed_wrapper, ())
        pending_wrapper = None
        pending_reason = None
    return PreparedManifest(
        manifest_sha256,
        provenance,
        pending_wrapper,
        pending_reason,
    )


def _prepare_run_directory(
    config: RunnerConfig,
    manifest: dict[str, object],
    *,
    runner_upgrade_reason: str | None = None,
) -> PreparedManifest:
    prepared = _prepare_manifest_directory(
        config.run_directory,
        manifest,
        runner_upgrade_reason=runner_upgrade_reason,
    )
    _atomic_write(
        config.run_directory / "takeaway-output-schema.json",
        _canonical_json_bytes(TAKEAWAY_SCHEMA),
    )
    compatibility.upgrade_worker(config.run_directory,
        _load_json(config.run_directory / 'manifest.json', 'run manifest'), _runner_identity(config))
    _atomic_write(
        config.run_directory / "mask-output-schema.json",
        _canonical_json_bytes(MASK_SCHEMA),
    )
    if config.prominence is not None:
        _atomic_write(
            config.run_directory / "prominence-output-schema.json",
            _canonical_json_bytes(PROMINENCE_SCHEMA),
        )
    return prepared


def _prepare_subject_run_directory(
    config: SubjectRunnerConfig,
    manifest: dict[str, object],
) -> PreparedManifest:
    prepared = _prepare_manifest_directory(config.run_directory, manifest)
    compatibility.upgrade_worker(config.run_directory,
        _load_json(config.run_directory / 'manifest.json', 'run manifest'), _runner_identity(config))
    _atomic_write(
        config.run_directory / "subject-output-schema.json",
        _canonical_json_bytes(SUBJECT_SCHEMA),
    )
    return prepared


def _load_latest_results(
    path: Path,
    manifest_sha256: str,
    provenance: RunnerProvenance,
    references: dict[str, SourceResultReference] | None = None,
    *, evidence=None,
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    latest: dict[str, dict[str, Any]] = {}
    def stream_rows():
        try:
            with path.open('rb') as stream:
                yield from ((line, None) for line in stream)
        except (OSError, UnicodeError) as error:
            raise RunnerError(f"could not read results ledger {path}: {error}") from error

    active_wrapper = provenance.initial_wrapper_sha256
    boundary_index = 0
    rows = stream_rows() if evidence is None else evidence
    for line_number, (line, parsed) in enumerate(rows, start=1):
        while (
            boundary_index < len(provenance.boundaries)
            and provenance.boundaries[boundary_index][0] <= line_number
        ):
            active_wrapper = provenance.boundaries[boundary_index][1]
            boundary_index += 1
        try:
            raw = json.loads(line) if evidence is None else parsed
        except (UnicodeError, json.JSONDecodeError) as error:
            raise RunnerError(
                f"results ledger line {line_number} is not valid JSON: {error}"
            ) from error
        record = _expect_object(raw, f"results ledger line {line_number}")
        if record.get("manifest_sha256") != manifest_sha256:
            raise RunnerError(
                f"results ledger line {line_number} belongs to another manifest"
            )
        recorded_wrapper = record.get("wrapper_sha256")
        if recorded_wrapper is None:
            if active_wrapper != provenance.initial_wrapper_sha256:
                raise RunnerError(
                    f"results ledger line {line_number} has invalid wrapper provenance"
                )
        elif (
            _expect_sha256(
                recorded_wrapper, f"results ledger line {line_number}.wrapper_sha256"
            )
            != active_wrapper
        ):
            raise RunnerError(
                f"results ledger line {line_number} has invalid wrapper provenance"
            )
        job_key = _expect_string(
            record.get("job_key"), f"results ledger line {line_number}.job_key"
        )
        if record.get("status") not in {"succeeded", "failed"}:
            raise RunnerError(f"results ledger line {line_number} has invalid status")
        attempt = record.get("attempt")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise RunnerError(f"results ledger line {line_number} has invalid attempt")
        previous = latest.get(job_key)
        if previous is not None and attempt != previous["attempt"] + 1:
            raise RunnerError(
                f"results ledger has a nonconsecutive attempt for `{job_key}`"
            )
        latest[job_key] = record
        if references is not None:
            references[job_key] = SourceResultReference(
                line=line_number,
                sha256=_sha256_bytes(line),
                attempt=attempt,
            )
    return latest


def _verify_pinned_reuse_file(
    path: Path,
    expected_sha256: str,
    label: str,
) -> None:
    try:
        actual_sha256 = _sha256_file(path)
    except OSError as error:
        raise RunnerError(f"could not verify reuse source {label} at {path}: {error}")
    if actual_sha256 != expected_sha256:
        raise RunnerError(
            f"reuse source {label} hash does not match its pinned SHA-256"
        )


def _verify_reuse_source_hashes(config: ReuseSourceConfig) -> None:
    _verify_pinned_reuse_file(
        config.input_path,
        config.input_sha256,
        "input",
    )
    _verify_pinned_reuse_file(
        config.run_directory / "manifest.json",
        config.manifest_sha256,
        "manifest",
    )
    _verify_pinned_reuse_file(
        config.run_directory / "results.jsonl",
        config.results_sha256,
        "results",
    )
    upgrades_path = config.run_directory / "runner-upgrades.jsonl"
    if config.runner_upgrades_sha256 is None:
        if upgrades_path.exists():
            raise RunnerError(
                "reuse source runner upgrades exist but no SHA-256 was pinned"
            )
    else:
        _verify_pinned_reuse_file(
            upgrades_path,
            config.runner_upgrades_sha256,
            "runner upgrades",
        )
    _verify_pinned_reuse_file(
        config.run_directory / "output.json",
        config.output_sha256,
        "output",
    )


def _source_stage_contracts(
    manifest: dict[str, Any],
) -> dict[str, SourceStageContract]:
    stages: dict[str, SourceStageContract] = {}
    for stage_name in ("takeaway", "prominence", "mask"):
        stage = _expect_object(
            manifest.get(stage_name), f"reuse source manifest.{stage_name}"
        )
        prompt = _expect_string(
            stage.get("prompt"), f"reuse source manifest.{stage_name}.prompt"
        )
        prompt_sha256 = _expect_sha256(
            stage.get("prompt_sha256"),
            f"reuse source manifest.{stage_name}.prompt_sha256",
        )
        if prompt_sha256 != _sha256_bytes(prompt.encode("utf-8")):
            raise RunnerError(
                f"reuse source manifest.{stage_name} prompt hash is invalid"
            )
        response_schema = _expect_object(
            stage.get("response_schema"),
            f"reuse source manifest.{stage_name}.response_schema",
        )
        response_schema_sha256 = _expect_sha256(
            stage.get("response_schema_sha256"),
            f"reuse source manifest.{stage_name}.response_schema_sha256",
        )
        if response_schema_sha256 != _sha256_bytes(
            _canonical_json_bytes(response_schema)
        ):
            raise RunnerError(
                f"reuse source manifest.{stage_name} response schema hash is invalid"
            )
        stages[stage_name] = SourceStageContract(
            prompt_version=_expect_string(
                stage.get("prompt_version"),
                f"reuse source manifest.{stage_name}.prompt_version",
            ),
            prompt=prompt,
            prompt_sha256=prompt_sha256,
            model=_expect_identifier(
                stage.get("model"), f"reuse source manifest.{stage_name}.model"
            ),
            reasoning_effort=_expect_identifier(
                stage.get("reasoning_effort"),
                f"reuse source manifest.{stage_name}.reasoning_effort",
            ),
            response_schema_sha256=response_schema_sha256,
        )
    return stages


def _compile_reusable_job(
    *,
    latest: dict[str, dict[str, Any]],
    references: dict[str, SourceResultReference],
    source_runner_version: str,
    stage_name: str,
    stage: SourceStageContract,
    note: Note,
    takeaway_index: int | None,
    rendered_prompt: str,
    output_response: list[str] | int,
) -> ReusableJob:
    job_key = _job_key(stage_name, note.note_id, takeaway_index)
    record = latest.get(job_key)
    if record is None or record.get("status") != "succeeded":
        raise RunnerError(
            f"reuse source output has no successful result for `{job_key}`"
        )
    if (
        record.get("schema_version") != SCHEMA_VERSION
        or record.get("execution") is not None
        or record.get("job_key") != job_key
        or record.get("stage") != stage_name
        or record.get("note_id") != note.note_id
        or record.get("takeaway_index") != takeaway_index
    ):
        raise RunnerError(f"reuse source result `{job_key}` has invalid job identity")
    rendered_prompt_sha256 = _sha256_bytes(rendered_prompt.encode("utf-8"))
    if record.get("model") != stage.model:
        raise RunnerError(f"reuse source result `{job_key}` has an invalid model")
    if record.get("reasoning_effort") != stage.reasoning_effort:
        raise RunnerError(
            f"reuse source result `{job_key}` has invalid reasoning effort"
        )
    if record.get("response_schema_sha256") != stage.response_schema_sha256:
        raise RunnerError(
            f"reuse source result `{job_key}` has an invalid response schema"
        )
    if record.get("prompt_sha256") != rendered_prompt_sha256:
        raise RunnerError(
            f"reuse source result `{job_key}` has an invalid rendered prompt hash"
        )
    if record.get("runner_version") != source_runner_version:
        raise RunnerError(
            f"reuse source result `{job_key}` has an invalid runner version"
        )
    if record.get("response") != output_response:
        raise RunnerError(
            f"reuse source result `{job_key}` does not match its output artifact"
        )
    reference = references.get(job_key)
    if reference is None:
        raise RunnerError(f"reuse source result `{job_key}` has no ledger reference")
    return ReusableJob(
        note_text=note.text,
        rendered_prompt_sha256=rendered_prompt_sha256,
        response=output_response,
        reference=reference,
    )


def _load_reuse_source(
    config: ReuseSourceConfig,
    target_notes: list[Note],
    provider: str,
) -> LoadedReuseSource:
    _verify_reuse_source_hashes(config)
    source_notes = _load_notes(config.input_path)
    manifest = _expect_object(
        _load_json(config.run_directory / "manifest.json", "reuse source manifest"),
        "reuse source manifest",
    )
    if manifest.get("schema_version") != PIPELINE_SCHEMA_VERSION:
        raise RunnerError("reuse source manifest must use pipeline schema version 2")
    if manifest.get("isolation") != _isolation_manifest():
        raise RunnerError("reuse source isolation contract is incompatible")
    if manifest.get("provider") != provider:
        raise RunnerError("reuse source provider must match the target provider")
    _expect_string(manifest.get("input_path"), "reuse source manifest.input_path")
    if manifest.get("input_sha256") != config.input_sha256:
        raise RunnerError(
            "reuse source manifest input hash does not match the pinned input"
        )
    initial_wrapper_sha256 = _expect_sha256(
        manifest.get("wrapper_sha256"), "reuse source manifest.wrapper_sha256"
    )
    if manifest.get("takeaway_envelope_version") != TAKEAWAY_ENVELOPE_VERSION:
        raise RunnerError("reuse source takeaway envelope is incompatible")
    if (
        manifest.get("takeaway_input_normalization_version")
        != TAKEAWAY_INPUT_NORMALIZATION_VERSION
    ):
        raise RunnerError("reuse source takeaway normalization is incompatible")
    if manifest.get("prominence_envelope_version") != "xml-complete-post-takeaway-v1":
        raise RunnerError("reuse source prominence envelope is incompatible")
    if (
        manifest.get("prominence_input_normalization_version")
        != TAKEAWAY_INPUT_NORMALIZATION_VERSION
    ):
        raise RunnerError("reuse source prominence normalization is incompatible")
    stages = _source_stage_contracts(manifest)
    source_runner_version = _expect_string(
        manifest.get("runner_version"), "reuse source manifest.runner_version"
    )

    provenance = _load_runner_upgrades(
        config.run_directory,
        manifest_sha256=config.manifest_sha256,
        initial_wrapper_sha256=initial_wrapper_sha256,
    )
    results_path = config.run_directory / "results.jsonl"
    references: dict[str, SourceResultReference] = {}
    latest = _load_latest_results(
        results_path,
        config.manifest_sha256,
        provenance,
        references,
    )

    output = _expect_object(
        _load_json(config.run_directory / "output.json", "reuse source output"),
        "reuse source output",
    )
    _expect_fields(
        output,
        "reuse source output",
        {"schema_version", "manifest_sha256", "notes"},
    )
    if output["schema_version"] != PIPELINE_SCHEMA_VERSION:
        raise RunnerError("reuse source output must use pipeline schema version 2")
    if output["manifest_sha256"] != config.manifest_sha256:
        raise RunnerError("reuse source output does not belong to the pinned manifest")
    raw_output_notes = output["notes"]
    if not isinstance(raw_output_notes, list):
        raise RunnerError("reuse source output.notes must be an array")
    if len(raw_output_notes) != len(source_notes):
        raise RunnerError(
            "reuse source output note order does not match its pinned input"
        )
    target_note_texts = {note.note_id: note.text for note in target_notes}
    jobs: dict[str, ReusableJob] = {}
    for index, (source_note, raw_note) in enumerate(
        zip(source_notes, raw_output_notes, strict=True)
    ):
        output_note = _expect_object(raw_note, f"reuse source output.notes[{index}]")
        _expect_fields(
            output_note,
            f"reuse source output.notes[{index}]",
            {"note_id", "takeaways"},
        )
        note_id = _expect_string(
            output_note["note_id"], f"reuse source output.notes[{index}].note_id"
        )
        if note_id != source_note.note_id:
            raise RunnerError(
                "reuse source output note order does not match its pinned input"
            )
        raw_takeaways = output_note["takeaways"]
        if not isinstance(raw_takeaways, list):
            raise RunnerError(
                f"reuse source output.notes[{index}].takeaways must be an array"
            )
        if target_note_texts.get(note_id) != source_note.text:
            continue
        takeaway_texts: list[str] = []
        validated_takeaways: list[tuple[str, int, list[str]]] = []
        for takeaway_index, raw_takeaway in enumerate(raw_takeaways):
            name = f"reuse source output note `{note_id}` takeaway {takeaway_index}"
            takeaway = _expect_object(raw_takeaway, name)
            _expect_fields(takeaway, name, {"text", "prominence", "fragments"})
            text = _expect_string(takeaway["text"], f"{name}.text")
            prominence = _validate_result_items(
                "prominence",
                takeaway["prominence"],
                source_note.text,
                stages["prominence"].prompt_version,
            )
            fragments = _validate_result_items(
                "mask",
                takeaway["fragments"],
                source_note.text,
                stages["mask"].prompt_version,
            )
            if not isinstance(prominence, int) or not isinstance(fragments, list):
                raise AssertionError("validated reuse source output has invalid types")
            takeaway_texts.append(text)
            validated_takeaways.append((text, prominence, fragments))
        takeaway_response = _validate_result_items(
            "takeaway",
            takeaway_texts,
            source_note.text,
            stages["takeaway"].prompt_version,
        )
        if not isinstance(takeaway_response, list):
            raise AssertionError("validated reuse source takeaways are not an array")
        takeaway_key = _job_key("takeaway", note_id, None)
        jobs[takeaway_key] = _compile_reusable_job(
            latest=latest,
            references=references,
            source_runner_version=source_runner_version,
            stage_name="takeaway",
            stage=stages["takeaway"],
            note=source_note,
            takeaway_index=None,
            rendered_prompt=_render_takeaway_prompt(
                stages["takeaway"].prompt, source_note
            ),
            output_response=takeaway_response,
        )
        for takeaway_index, (text, prominence, fragments) in enumerate(
            validated_takeaways
        ):
            for stage_name, response, rendered_prompt in (
                (
                    "prominence",
                    prominence,
                    _render_prominence_prompt(
                        stages["prominence"].prompt, source_note, text
                    ),
                ),
                (
                    "mask",
                    fragments,
                    _render_mask_prompt(stages["mask"].prompt, source_note, text),
                ),
            ):
                job_key = _job_key(stage_name, note_id, takeaway_index)
                jobs[job_key] = _compile_reusable_job(
                    latest=latest,
                    references=references,
                    source_runner_version=source_runner_version,
                    stage_name=stage_name,
                    stage=stages[stage_name],
                    note=source_note,
                    takeaway_index=takeaway_index,
                    rendered_prompt=rendered_prompt,
                    output_response=response,
                )

    return LoadedReuseSource(
        config=config,
        stages=stages,
        jobs=jobs,
    )


def _append_runner_upgrade(
    run_directory: Path,
    prepared: PreparedManifest,
) -> RunnerProvenance:
    if prepared.pending_wrapper_sha256 is None:
        return prepared.provenance
    if prepared.pending_reason is None:
        raise AssertionError("pending runner upgrade has no audit reason")
    result_size, result_sha256, result_line_count, _ = compatibility._results_prefix(run_directory)
    first_result_line = result_line_count + 1
    _append_json_line(
        run_directory / "runner-upgrades.jsonl",
        {
            "schema_version": 2,
            "manifest_sha256": prepared.manifest_sha256,
            "from_wrapper_sha256": prepared.provenance.active_wrapper_sha256,
            "to_wrapper_sha256": prepared.pending_wrapper_sha256,
            "first_result_line": first_result_line,
            "results_prefix_bytes": result_size,
            "results_prefix_sha256": result_sha256,
            "reason": prepared.pending_reason,
        },
    )
    return RunnerProvenance(
        prepared.provenance.initial_wrapper_sha256,
        prepared.pending_wrapper_sha256,
        prepared.provenance.boundaries
        + ((first_result_line, prepared.pending_wrapper_sha256),),
    )


def _validate_string_array(
    value: object,
    name: str,
    *,
    allow_empty: bool,
) -> list[str]:
    if not isinstance(value, list) or (not allow_empty and not value):
        qualifier = "an array" if allow_empty else "a nonempty array"
        raise RunnerError(f"model {name} response must be {qualifier} of strings")
    result: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise RunnerError(
                f"model {name} response item {index} must be a nonempty string"
            )
        result.append(item)
    return result


def _validate_result_items(
    stage_name: str,
    value: object,
    note_text: str,
    prompt_version: str,
) -> list[str] | int:
    if stage_name in {"takeaway", "subject"}:
        return _validate_string_array(value, stage_name, allow_empty=True)
    if stage_name == "prominence":
        if isinstance(value, bool) or not isinstance(value, int):
            raise RunnerError("model prominence response must be an integer")
        if not 0 <= value <= 100:
            raise RunnerError("model prominence response must be between 0 and 100")
        return value
    fragments = _validate_string_array(value, "mask", allow_empty=False)
    if prompt_version in URL_FREE_MASK_PROMPT_VERSIONS:
        for index, fragment in enumerate(fragments):
            if URL_SCHEME_PATTERN.search(fragment):
                raise RunnerError(
                    f"mask fragment {index} must not contain a URL under "
                    f"the frozen {prompt_version} contract"
                )
    cursor = 0
    for index, fragment in enumerate(fragments):
        position = note_text.find(fragment, cursor)
        if position < 0:
            raise RunnerError(
                f"mask fragment {index} `{fragment}` is not present in the "
                "source note after the preceding fragment"
            )
        cursor = position + len(fragment)
    return fragments


def _validate_response(
    stage_name: str,
    value: object,
    note_text: str,
    prompt_version: str,
) -> list[str] | int:
    response_object = _expect_object(value, f"model {stage_name} response")
    field = {
        "takeaway": "takeaways",
        "subject": "subjects",
        "prominence": "prominence",
        "mask": "fragments",
    }[stage_name]
    _expect_fields(response_object, f"model {stage_name} response", {field})
    return _validate_result_items(
        stage_name,
        response_object[field],
        note_text,
        prompt_version,
    )


def _job_key(
    stage_name: str,
    note_id: str,
    takeaway_index: int | None,
) -> str:
    if stage_name == "prompt":
        return f"prompt:{note_id}"
    if stage_name == "subject":
        return f"subject:{note_id}"
    if stage_name == "takeaway":
        return f"takeaway:{note_id}"
    if takeaway_index is None:
        raise AssertionError(f"{stage_name} job requires a takeaway index")
    return f"{stage_name}:{note_id}:{takeaway_index}"


def _build_exec_argv(
    config: Any,
) -> list[str]:
    arguments = [str(config.runner_binary), "run", "--send"]
    if config.auth_file is not None:
        arguments.extend(("--auth-file", str(config.auth_file)))
    return arguments


def _experiment_scope_loader(config, stage):
    """Discover native account/transport scopes before admitting a worker."""
    if config.provider not in {'openai-codex', 'openrouter'} or _runner_identity(config).get('experiment_scopes') is not True:
        return None
    def load_scopes():
        from experiment_runner.provider_backoff import SharedProviderBackoff
        request = dict(provider=config.provider, model=stage.model,
                       reasoning=stage.reasoning_effort, prompt='', timeout_seconds=config.timeout_seconds)
        completed = subprocess.run([str(config.runner_binary), 'scopes'],
            input=json.dumps(request), text=True, capture_output=True,
            env=_runner_environment(), timeout=30)
        if completed.returncode != 0:
            raise ExperimentAdmissionUnavailable('Experiment scopes unavailable before dispatch; no worker started')
        try:
            scopes = json.loads(completed.stdout)
            SharedProviderBackoff._keys(scopes)
        except (ValueError, TypeError) as error:
            raise ExperimentAdmissionUnavailable('Invalid experiment scopes; no worker started') from error
        return scopes
    return load_scopes


def _experiment_endpoint_loader(config, identity):
    """Read trusted worker endpoint capacity before launch, without inference."""
    if identity.get('endpoint_admission') is not True:
        return None
    def load_endpoint():
        try:
            completed = subprocess.run([str(config.runner_binary), 'admission'],
                text=True, capture_output=True, env=_runner_environment(), timeout=30)
            if completed.returncode != 0:
                raise ValueError('endpoint metadata command failed')
            endpoint = _expect_object(json.loads(completed.stdout), 'endpoint admission')
            _expect_fields(endpoint, 'endpoint admission', {'scope', 'capacity'})
            _expect_sha256(endpoint['scope'], 'endpoint scope')
            if type(endpoint['capacity']) is not int or not 1 <= endpoint['capacity'] <= scheduling.LIMIT:
                raise ValueError('invalid endpoint capacity')
        except (ValueError, RunnerError, subprocess.SubprocessError, OSError) as error:
            raise ExperimentAdmissionUnavailable(
                'Experiment endpoint admission metadata unavailable; no worker started') from error
        return endpoint
    from experiment_runner import scheduling
    return load_endpoint


def _same_json(left: Any, right: Any) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _same_json(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _same_json(a, b) for a, b in zip(left, right)
        )
    if type(left) is bool or type(right) is bool:
        return type(left) is type(right) and left == right
    return left == right


def _native_content_id(kind: str, content: object) -> str:
    import uuid
    name = kind + "\0" + json.dumps(content, ensure_ascii=False, separators=(",", ":"))
    return kind + "_" + str(uuid.uuid5(uuid.NAMESPACE_DNS, name))


def _native_message(role: str, text: str) -> dict[str, Any]:
    content = [{"type": "input_text", "text": text}]
    return {"type": "message", "id": _native_content_id("msg", {"role": role, "content": content}),
            "role": role, "content": content,
            "internal_chat_message_metadata_passthrough": {
                "content_item_kinds": ["model.base_instructions" if role == "developer" else "user.text"]}}


def _wire_prompt(result: dict[str, Any], provider: str, *, cache_prefix: str | None = None) -> str:
    body = _expect_object(
        _expect_object(result.get("request"), "model request").get("body"),
        "model request body",
    )
    messages = body.get("input") if provider == "openai-codex" else body.get("messages")
    if cache_prefix is not None:
        if provider != "openai-codex" or not isinstance(messages, list) or len(messages) not in (2, 3):
            raise RunnerError("model result native cache prefix input differs")
        tools = {"type": "additional_tools", "id": _native_content_id("at", []),
                 "role": "developer", "tools": []}
        if messages[:2] != [tools, _native_message("developer", cache_prefix)]:
            raise RunnerError("model result native cache prefix or item IDs differ")
        suffix = ""
        if len(messages) == 3:
            try:
                suffix = messages[2]["content"][0]["text"]
            except (KeyError, IndexError, TypeError) as error:
                raise RunnerError("model result native suffix is invalid") from error
            if not isinstance(suffix, str) or not suffix or messages[2] != _native_message("user", suffix):
                raise RunnerError("model result native suffix or item ID differs")
        return cache_prefix + suffix
    if (
        not isinstance(messages, list)
        or len(messages) != 1
        or not isinstance(messages[0], dict)
        or messages[0].get("role") != "user"
    ):
        raise RunnerError(
            "model result request must contain only the caller's user message"
        )
    content = messages[0].get("content")
    text = (
        content
        if isinstance(content, str)
        else content[0].get("text")
        if isinstance(content, list)
        and len(content) == 1
        and isinstance(content[0], dict)
        else None
    )
    if not isinstance(text, str):
        raise RunnerError("model result request contains invalid prompt content")
    return text


def _validate_model_result(
    config: Any,
    stage: Any,
    prompt: str,
    schema: object,
    identity: dict[str, Any],
    result: dict[str, Any],
) -> None:
    expected = {
        "provider": config.provider,
        "model": stage.model,
        "reasoning": stage.reasoning_effort,
        "runner_version": identity["runner_version"],
        "runner_sha256": identity["runner_sha256"],
        "prompt_sha256": _sha256_bytes(prompt.encode("utf-8")),
        "valid": True,
        "stop_reason": "stop",
        "http_attempts": 1,
        "error": None,
    }
    if identity.get('request_code_sha256') is not None:
        expected['request_code_sha256'] = identity['request_code_sha256']
    if any(not _same_json(result.get(key), value) for key, value in expected.items()):
        raise RunnerError(
            "model runner did not return a valid completed response bound to the request"
        )
    transport = getattr(config, "transport", "sse")
    if result.get("transport", "sse") != transport:
        raise RunnerError("model result transport differs from frozen request")
    raw = result.get("raw_text")
    if not isinstance(raw, str) or not raw.strip():
        raise RunnerError("model runner final response is empty")
    body = _expect_object(
        _expect_object(result.get("request"), "model request").get("body"),
        "model request body",
    )
    key = getattr(config, "prompt_cache_key", None)
    prefix = getattr(config, "prompt_cache_prefix", None)
    if config.provider == "openai-codex":
        headers = _expect_object(result["request"].get("headers", {}), "model request headers")
        session = [v for k, v in headers.items() if k.lower() == 'session-id']
        client = [v for k, v in headers.items() if k.lower() == 'x-client-request-id']
        if key is not None:
            _validate_prompt_cache_key(key, config.provider, transport)
            if body.get('prompt_cache_key') != key or session != [key] or client != [key]:
                raise RunnerError("model result cache affinity differs from frozen request")
        elif 'prompt_cache_key' in body or session:
            raise RunnerError("model result unexpectedly adds cache affinity")
        if prefix is not None:
            if (headers.get("x-openai-internal-codex-responses-lite") != "true"
                    or headers.get("x-codex-routing-hint") != "model=gpt-6.1-sol"
                    or headers.get("thread-id") != key
                    or body.get("client_metadata") != {"session_id": key, "thread_id": key}
                    or (body.get("reasoning") or {}).get("context") != "all_turns"
                    or body.get("tool_choice") != "auto"
                    or body.get("parallel_tool_calls") is not False
                    or body.get("include") != ["reasoning.encrypted_content"]
                    or "instructions" in body or "tools" in body):
                raise RunnerError("model result native cache prefix settings differ")
    if transport == "websocket" and (
        result["request"].get("method") != "GET"
        or not result["request"].get("url", "").startswith(("ws://", "wss://"))
        or body.get("type") != "response.create"
        or "stream" in body or "background" in body
        or result.get("response_metadata", {}).get("http_status") != 101
    ):
        raise RunnerError("model result does not establish the requested WebSocket exchange")
    wire_schema = (
        (body.get("text") or {}).get("format", {}).get("schema")
        if config.provider == "openai-codex"
        else (body.get("response_format") or {}).get("json_schema", {}).get("schema")
    )
    systemone = config.provider == "openrouter" and stage.model == "typesafe/jev-1.13"
    if not _same_json(wire_schema, None if systemone else schema):
        raise RunnerError("model result wire schema differs from caller schema")
    if schema is None:
        if result.get("schema_sha256") is not None:
            raise RunnerError("model result unexpectedly reports a schema")
    else:
        _expect_sha256(result.get("schema_sha256"), "model result.schema_sha256")
    if systemone:
        expected_body = {"model": stage.model, **json.loads(prompt)}
        if not _same_json(body, expected_body) or not result["request"].get("url", "").endswith("/api/v1/systemone"):
            raise RunnerError("System One request differs from exact state/questions")
        text = prompt
    else:
        text = _wire_prompt(result, config.provider, cache_prefix=prefix)
    if (
        text != prompt
        or body.get("model") != stage.model
        or body.get("instructions", "")
        or body.get("tools")
        or body.get("previous_response_id")
    ):
        raise RunnerError("model result request differs from the exact isolated prompt")
    output = raw if schema is None and not systemone else json.loads(raw)
    if not _same_json(result.get("output"), output):
        raise RunnerError("model result output differs from its raw response")


def _invoke_prompt_worker(config, argv, request, directory, *, worker_identity):
    """Bound the active request, while allowing shared admission to wait first."""
    marker = 'oracle-admission-' + os.urandom(16).hex()
    from experiment_runner.scheduling import LIMIT, current_slot
    slot = current_slot()
    environment = {**_runner_environment(),
                   'ORACLE_EXPERIMENT_PYTHON': sys.executable,
                   'ORACLE_EXPERIMENT_ADMISSION_MARKER': marker}
    if slot and slot['provider_ticket']:
        environment['ORACLE_EXPERIMENT_CONCURRENCY'] = str(LIMIT)
        environment['ORACLE_EXPERIMENT_PROVIDER_TICKET'] = slot['provider_ticket']
    environment['ORACLE_EXPERIMENT_EXPECTED_RUNNER_SHA256'] = worker_identity['runner_sha256']
    waiting_line, admitted_line = (marker + ':waiting\n').encode(), (marker + ':admitted\n').encode()
    unavailable_line = marker + ':unavailable\n'
    deadline = time.monotonic() + config.timeout_seconds + 30
    admitted = False
    data = _ordered_json_bytes(request).decode('utf-8')
    with subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, cwd=directory, env=environment) as child:
        try:
            if slot:
                slot['queue'].worker_started(slot['identifier'], child.pid)
            while True:
                try:
                    stdout, stderr = child.communicate(input=data, timeout=0.25)
                    break
                except subprocess.TimeoutExpired as partial:
                    data = None
                    observed = partial.stderr or b''
                    if admitted_line in observed and not admitted:
                        admitted = True
                        deadline = time.monotonic() + config.timeout_seconds + 30
                    elif waiting_line in observed and not admitted:
                        deadline = None
                    if deadline is not None and time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired(argv, config.timeout_seconds + 30)
        finally:
            if child.poll() is None:
                # The native launcher forwards termination to its selected Pi
                # child; keep the existing process-group interrupt behavior.
                child.terminate()
                try:
                    child.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.communicate()
    if unavailable_line in stderr.splitlines(keepends=True):
        raise ExperimentAdmissionUnavailable(
            'Experiment admission unavailable; restore the local scope/status service and admission state before running')
    stderr = ''.join(line for line in stderr.splitlines(keepends=True)
                     if line not in {waiting_line.decode(), admitted_line.decode()})
    return subprocess.CompletedProcess(argv, child.returncode, stdout, stderr)


def _invoke_model(
    config: Any, stage: Any, prompt: str, schema: object,
    *, receipt_files: ReceiptFiles | None = None,
) -> dict[str, Any]:
    receipt_files = receipt_files or ReceiptFiles()
    _validate_auth_file(config.auth_file)
    identity = _runner_identity(config)
    from experiment_runner.scheduling import current_slot
    slot = current_slot()
    if slot is not None:
        slot['queue'].check_worker(identity)
    request = {
        "provider": config.provider,
        "model": stage.model,
        "reasoning": stage.reasoning_effort,
        "prompt": prompt,
        "timeout_seconds": config.timeout_seconds,
    }
    if getattr(config, "transport", "sse") != "sse":
        request["transport"] = config.transport
    if getattr(config, "prompt_cache_key", None) is not None:
        request["prompt_cache_key"] = config.prompt_cache_key
    if getattr(config, "prompt_cache_prefix", None) is not None:
        request["prompt_cache_prefix"] = config.prompt_cache_prefix
    if schema is not None:
        request["schema"] = schema
    if config.max_output_tokens is not None:
        request["max_output_tokens"] = config.max_output_tokens
    argv = _build_exec_argv(config)
    started = time.monotonic()
    stdout = stderr = ""
    exit_code = None
    result: dict[str, Any] | None = None
    error_message = None
    try:
        manifest = _expect_object(
            receipt_files.json(config.run_directory / "manifest.json"),
            "run manifest",
        )
        if not compatibility.compatible(manifest, identity):
            raise RunnerError("model runner changed since the run manifest was created")
        if slot is not None and config.run_directory != slot['queue'].path.parent:
            compatibility.upgrade_worker(config.run_directory, manifest, identity)
        with tempfile.TemporaryDirectory(prefix="model-call-") as temporary:
            completed = _invoke_prompt_worker(config, argv, request, temporary, worker_identity=identity)
        stdout, stderr, exit_code = (
            completed.stdout,
            completed.stderr,
            completed.returncode,
        )
        if stdout.strip():
            result = _expect_object(json.loads(stdout), "model result")
        if exit_code != 0:
            raise RunnerError(f"model runner exited with status {exit_code}")
        if result is None:
            raise RunnerError("model runner returned no result")
        _validate_model_result(config, stage, prompt, schema, identity, result)
    except (ExperimentAdmissionUnavailable, ExperimentPaused):
        # Stop scheduling without saving an unadmitted case as a provider failure.
        # Its existing worker reservation remains conservatively consumed.
        raise
    except (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        AttributeError,
        IndexError,
        RunnerError,
        subprocess.TimeoutExpired,
    ) as error:
        error_message = str(error)
    output = (
        None
        if result is None or not isinstance(result.get("raw_text"), str)
        else result["raw_text"]
    )
    return {
        "argv": argv,
        "exit_code": exit_code,
        "elapsed_seconds": time.monotonic() - started,
        "provider": config.provider,
        "runner_sha256": identity["runner_sha256"],
        **({'request_code_sha256': identity['request_code_sha256']}
           if 'request_code_sha256' in identity else {}),
        "model_result": result,
        "usage": None if result is None else result.get("usage"),
        "stdout_json": stdout,
        "stdout_sha256": _sha256_bytes(stdout.encode("utf-8")),
        "stderr": stderr,
        "stderr_sha256": _sha256_bytes(stderr.encode("utf-8")),
        "final_output_raw": output,
        "final_output_sha256": None
        if output is None
        else _sha256_bytes(output.encode("utf-8")),
        "error": error_message,
    }


class ReceiptFiles:
    """Operation-scoped file reads and provenance, never receipt verdicts.

    Keep the executing manifest and at most 16 MiB of other JSON source bytes.
    Evicted JSON is parsed again when needed. File digests retain only a stamp
    and hash; unchanged files need no repeat parsing or hashing. JSONL retains only
    line offsets, not every receipt body. Every requested line is read and
    verified normally. A new operation starts with an empty cache.
    """

    def __init__(self, *, manifest_path=None):
        self._json = {}
        self._json_bytes = 0
        self._hashes = {}
        self._manifest_path = Path(manifest_path) if manifest_path is not None else None
        self._offsets = {}
        self._first_lines = {}
        self._upgrades = {}
        self._worker_upgrades = {}
        self._lock = threading.RLock()

    @staticmethod
    def _stamp(path):
        info = path.stat()
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    def _entry(self, path):
        path = Path(path)
        stamp = self._stamp(path)
        entry = self._json.pop(path, None)
        if entry is not None and path != self._manifest_path:
            self._json_bytes -= entry['stamp'][2]
        if entry is None or entry['stamp'] != stamp:
            raw = path.read_bytes()
            if self._stamp(path) != stamp:
                raise RunnerError(f'source changed while reading: {path}')
            entry = dict(stamp=stamp, value=json.loads(raw), sha256=_sha256_bytes(raw))
        self._hashes[path] = (entry['stamp'], entry['sha256'])
        if path == self._manifest_path or stamp[2] <= 16 * 1024 * 1024:
            if path != self._manifest_path:
                while self._json_bytes + stamp[2] > 16 * 1024 * 1024:
                    oldest = next(key for key in self._json if key != self._manifest_path)
                    removed = self._json.pop(oldest)
                    self._json_bytes -= removed['stamp'][2]
                self._json_bytes += stamp[2]
            self._json[path] = entry
        return entry

    def json(self, path):
        with self._lock:
            return self._entry(path)['value']

    def sha256(self, path):
        path = Path(path)
        with self._lock:
            stamp = self._stamp(path)
            cached = self._hashes.get(path)
            if cached is not None and cached[0] == stamp:
                return cached[1]
            digest = _sha256_file(path)
            if self._stamp(path) != stamp:
                raise RunnerError(f'source changed while hashing: {path}')
            self._hashes[path] = (stamp, digest)
            return digest

    def prompt(self, path, identifier):
        with self._lock:
            entry = self._entry(path)
            if 'prompts' not in entry:
                entry['prompts'] = {}
                for prompt in entry['value']['prompts']:
                    entry['prompts'].setdefault(prompt['prompt_id'], prompt)
            return entry['prompts'][identifier]

    def lines(self, path):
        path = Path(path)
        with self._lock:
            stamp = self._stamp(path)
            offsets, offset = [], 0
            with path.open('rb') as stream:
                for line in stream:
                    offsets.append(offset)
                    offset += len(line)
                    yield line
            # A live source can append while history is indexed. Its candidates
            # still need verification; do not cache offsets from a moving file.
            if self._stamp(path) == stamp:
                self._offsets[path] = (stamp, offsets)

    def line(self, path, number):
        path = Path(path)
        with self._lock:
            stamp = self._stamp(path)
            if path not in self._offsets or self._offsets[path][0] != stamp:
                for _ in self.lines(path):
                    pass
            indexed = self._offsets.get(path)
            if indexed is None or indexed[0] != stamp:
                with path.open('rb') as stream:
                    return next(line for i, line in enumerate(stream, 1) if i == number)
            if type(number) is not int or not 1 <= number <= len(indexed[1]):
                raise RunnerError('request history source line is missing')
            with path.open('rb') as stream:
                stream.seek(indexed[1][number - 1])
                line = stream.readline()
            if self._stamp(path) != stamp:
                # A concurrently appended/replaced source must be read by line
                # number again; the caller still verifies the exact line hash.
                with path.open('rb') as stream:
                    return next(line for i, line in enumerate(stream, 1) if i == number)
            return line

    def _first_line_index(self, path):
        stamp = self._stamp(path)
        entry = self._first_lines.get(path)
        if entry is not None and entry['stamp'] == stamp:
            return entry
        first, offset, count, terminated = {}, 0, 0, True
        with path.open('rb') as stream:
            for count, line in enumerate(stream, 1):
                first.setdefault(_sha256_bytes(line), (count, offset))
                offset += len(line)
                terminated = line.endswith(b'\n')
        if self._stamp(path) != stamp:
            raise RunnerError(f'source changed while indexing: {path}')
        entry = dict(stamp=stamp, first=first, count=count, terminated=terminated)
        self._first_lines[path] = entry
        return entry

    def appended(self, path, payload, offset, before, after):
        """Advance only a verified own append; unknown changes rebuild the index."""
        path = Path(path)
        with self._lock:
            entry = self._first_lines.get(path)
            if (entry is None or entry['stamp'] != before or not entry['terminated']
                    or offset != before[2] or after[:2] != before[:2]
                    or after[2] != before[2] + len(payload)):
                self._first_line_index(path)
                return
            entry['count'] += 1
            entry['first'].setdefault(_sha256_bytes(payload), (entry['count'], offset))
            entry['stamp'] = after
            entry['terminated'] = payload.endswith(b'\n')

    def first_line(self, path, expected):
        """Find the first exact saved bytes, retaining only hashes and positions."""
        path = Path(path)
        with self._lock:
            entry = self._first_line_index(path)
            number, offset = entry['first'].get(_sha256_bytes(expected), (None, None))
            if number is None:
                raise StopIteration
            with path.open('rb') as stream:
                stream.seek(offset)
                line = stream.readline()
            if self._stamp(path) != entry['stamp']:
                raise RunnerError(f'source changed while reading: {path}')
            if line == expected:
                return number
            # A hash candidate is not an equality verdict. Preserve first-exact
            # semantics even if distinct lines ever share the same digest.
            with path.open('rb') as stream:
                number = next(i for i, line in enumerate(stream, 1) if line == expected)
            if self._stamp(path) != entry['stamp']:
                raise RunnerError(f'source changed while reading: {path}')
            return number

    def provenance(self, directory, *, manifest_sha256, initial_wrapper_sha256):
        with self._lock:
            paths = [directory/'runner-upgrades.jsonl', directory/'results.jsonl']
            stamps = tuple(self._stamp(p) if p.exists() else None for p in paths)
            key = (directory, manifest_sha256, initial_wrapper_sha256)
            saved = self._upgrades.get(key)
            if saved is None or saved[0] != stamps:
                value = _load_runner_upgrades(directory, manifest_sha256=manifest_sha256,
                                             initial_wrapper_sha256=initial_wrapper_sha256)
                saved = (stamps, value)
                if stamps == tuple(self._stamp(p) if p.exists() else None for p in paths):
                    self._upgrades[key] = saved
            return saved[1]

    def worker_provenance(self, directory, manifest):
        with self._lock:
            directory = Path(directory)
            paths = [directory / name for name in ('manifest.json', 'worker-upgrades.jsonl', 'results.jsonl')]
            stamps = tuple(self._stamp(p) if p.exists() else None for p in paths)
            saved = self._worker_upgrades.get(directory)
            if saved is None or saved[0] != stamps:
                saved = (stamps, compatibility.worker_history(directory, manifest))
                if stamps == tuple(self._stamp(p) if p.exists() else None for p in paths):
                    self._worker_upgrades[directory] = saved
            return saved[1]


def validate_model_receipt(
    config: Any,
    stage: Any,
    prompt: str,
    schema: object,
    manifest: dict[str, Any],
    record: dict[str, Any],
    *, files: ReceiptFiles | None = None,
) -> None:
    """Verify a saved native receipt without credentials or model requests."""
    try:
        worker = compatibility.receipt_identity(config.run_directory, manifest, record,
            identities=files.worker_provenance(config.run_directory, manifest) if files else None)
    except ValueError as error:
        raise RunnerError(str(error)) from error
    expected = {
        "model": stage.model,
        "reasoning_effort": stage.reasoning_effort,
        "manifest_sha256": (files.sha256 if files else _sha256_file)(config.run_directory / "manifest.json"),
        "response_schema_sha256": None
        if schema is None
        else _sha256_bytes(
            _ordered_json_bytes(schema)
            if isinstance(stage, PromptRunnerConfig)
            else _canonical_json_bytes(schema)
        ),
        "provider": config.provider,
        "runner_version": worker["runner_version"],
        "runner_sha256": worker["runner_sha256"],
        "error": None,
        "prompt_sha256": _sha256_bytes(prompt.encode("utf-8")),
        "exit_code": 0,
        "argv": _build_exec_argv(config),
    }
    if record.get("status") != "succeeded" or any(
        not _same_json(record.get(k), v) for k, v in expected.items()
    ):
        raise RunnerError("saved receipt differs from expected model invocation")
    for name, field in (
        ("stdout", "stdout_json"),
        ("stderr", "stderr"),
        ("final_output", "final_output_raw"),
    ):
        value = record.get(field)
        if not isinstance(value, str) or record.get(name + "_sha256") != _sha256_bytes(
            value.encode("utf-8")
        ):
            raise RunnerError("saved receipt output hash differs")
    try:
        result = _expect_object(json.loads(record["stdout_json"]), "saved model result")
        if (
            not _same_json(result, record.get("model_result"))
            or not _same_json(result.get("usage"), record.get("usage"))
            or result.get("raw_text") != record.get("final_output_raw")
        ):
            raise RunnerError(
                "saved receipt model result, usage, or raw output differs"
            )
        _validate_model_result(config, stage, prompt, schema, worker, result)
    except (ValueError, TypeError, AttributeError, IndexError) as error:
        raise RunnerError("saved receipt model result is malformed") from error


def validate_prompt_receipt(
    config: PromptRunnerConfig, manifest: dict[str, Any], record: dict[str, Any],
    *, files: ReceiptFiles | None = None,
) -> None:
    """Bind one saved prompt to its explicit caller config without sending it."""
    if len(config.prompts) != 1:
        raise RunnerError("receipt verification requires a single configured prompt")
    prompt = config.prompts[0]
    if record.get("execution") == "cross_run_reuse":
        key = _prompt_request_key(manifest, prompt.text)
        source = _read_request_source(record.get("reused_from"), key, files=files)
        expected = _reused_prompt_record(config, manifest, prompt, record["reused_from"], source, files=files)
        provenance = (files.provenance if files else _load_runner_upgrades)(
            config.run_directory,
            manifest_sha256=expected["manifest_sha256"],
            initial_wrapper_sha256=manifest["wrapper_sha256"],
        )
        allowed_wrappers = {provenance.initial_wrapper_sha256}
        allowed_wrappers.update(wrapper for _, wrapper in provenance.boundaries)
        if record.get("wrapper_sha256") not in allowed_wrappers:
            raise RunnerError("saved reused response has unverified runner provenance")
        expected["wrapper_sha256"] = record["wrapper_sha256"]
        if record.get('request_sha256') == _prompt_request_key(manifest, prompt.text, legacy=True):
            expected['request_sha256'] = record['request_sha256']
        if not _same_json(record, expected):
            raise RunnerError("saved reused response differs from its source receipt")
        return
    validate_model_receipt(
        config, config, prompt.text, config.output_schema, manifest, record, files=files
    )
    expected_schema = (
        None
        if config.output_schema is None
        else _sha256_bytes(_ordered_json_bytes(config.output_schema))
    )
    if (
        record.get("stage") != "prompt"
        or record.get("note_id") != prompt.prompt_id
        or record.get("job_key") != "prompt:" + prompt.prompt_id
        or record.get("response_schema_sha256") != expected_schema
        or record.get("response") != record.get("final_output_raw")
    ):
        raise RunnerError("saved receipt differs from expected prompt response")


def _prompt_request_key(manifest: dict[str, Any], prompt: str, *, legacy=False) -> str:
    """Content identity, independent of run paths, job IDs and prompt version labels."""
    request = {name: manifest[name] for name in (
        "provider", "model", "reasoning_effort", "runner_version", "runner_sha256",
    )}
    request_code = compatibility.request_code(manifest)
    if request_code is not None and not legacy:
        del request['runner_sha256']
        request['request_code_sha256'] = request_code
    request.update(prompt=prompt,
                   timeout_seconds=float(manifest["timeout_seconds"]),
                   max_output_tokens=manifest.get("max_output_tokens"),
                   response_schema_sha256=manifest.get("response_schema_sha256"))
    # Preserve historical SSE keys; alternate transports are distinct requests.
    if manifest.get("transport", "sse") != "sse":
        request["transport"] = manifest["transport"]
    if manifest.get("prompt_cache_key") is not None:
        request["prompt_cache_key"] = manifest["prompt_cache_key"]
    if manifest.get("prompt_cache_prefix") is not None:
        request["prompt_cache_prefix"] = manifest["prompt_cache_prefix"]
    if manifest.get('cache_warmup'):
        request['cache_warmup'] = True
    return _sha256_bytes(_canonical_json_bytes(request))


def _request_history_options(
    config: PromptRunnerConfig, manifest: dict[str, Any], *,
    repeat: bool, history_root: Path | None,
) -> None:
    saved = config.run_directory / "manifest.json"
    if saved.exists() and not repeat and history_root is None:
        previous = _load_json(saved, "run manifest").get("request_reuse")
        if previous is not None:
            manifest["request_reuse"] = previous
            return
    root = config.config_path.parent
    if history_root is not None:
        root = history_root.expanduser().resolve()
    else:
        root = next((p for p in (root, *root.parents) if (p / ".git").exists()), root)
    if not root.is_dir():
        raise RunnerError(f"request history root is not a directory: {root}")
    manifest["request_reuse"] = {"history_root": str(root), "intentional_repeat": repeat}


def _read_request_source(reference: object, request_key: str, *,
                         files: ReceiptFiles | None = None) -> dict[str, Any]:
    """Read and verify the original native receipt; never trust just a cached answer."""
    try:
        files = files or ReceiptFiles()
        reference = _expect_object(reference, "request history reference")
        path = Path(reference["results_path"])
        manifest = files.json(path.parent / "manifest.json")
        line = files.line(path, reference["result_line"])
        if _sha256_bytes(line) != reference["result_sha256"]:
            raise RunnerError("request history source receipt hash differs")
        record = json.loads(line)
        if record.get("execution") is not None:
            raise RunnerError("request history source must be an original model call")
        # stdout retains the actual schema property order, unlike the sorted manifest.
        result = json.loads(record["stdout_json"])
        body = result["request"]["body"]
        schema = ((body.get("text") or {}).get("format", {}).get("schema")
                  if manifest["provider"] == "openai-codex" else
                  (body.get("response_format") or {}).get("json_schema", {}).get("schema"))
        if manifest["provider"] == "openrouter" and manifest["model"] == "typesafe/jev-1.13":
            # System One schemas validate locally, so none is sent on the wire.
            # Recover its original property order; immutable receipt hashes below
            # still reject a changed schema instead of trusting the config file.
            schema = files.json(Path(manifest["config_path"])).get("output_schema")
        prompt = next(p for p in manifest['prompts'] if p['prompt_id'] == record['note_id'])
        config = PromptRunnerConfig(
            config_path=Path(manifest["config_path"]),
            runner_binary=Path(manifest["runner_binary"]),
            auth_file=Path(manifest["auth_file"]) if manifest["auth_file"] else None,
            provider=manifest["provider"], timeout_seconds=manifest["timeout_seconds"],
            max_output_tokens=manifest.get("max_output_tokens"), run_directory=path.parent,
            model=manifest["model"], reasoning_effort=manifest["reasoning_effort"],
            prompts=(PromptJob(prompt["prompt_id"], prompt["text"]),), output_schema=schema,
            transport=manifest.get("transport", "sse"),
            prompt_cache_key=manifest.get("prompt_cache_key"),
            prompt_cache_prefix=manifest.get("prompt_cache_prefix"),
        )
        if (record["manifest_sha256"] != reference["manifest_sha256"]
                or _prompt_request_key(manifest, prompt["text"]) != request_key
                or record.get("request_sha256", request_key) not in {
                    request_key, _prompt_request_key(manifest, prompt['text'], legacy=True)}
                or manifest.get("response_schema_sha256") != record.get("response_schema_sha256")):
            raise RunnerError("request history source settings differ")
        validate_prompt_receipt(config, manifest, record, files=files)
        return record
    except (OSError, ValueError, KeyError, TypeError, AttributeError, StopIteration, RunnerError) as error:
        raise RunnerError(f"cannot verify request history source: {error}") from error


def _reused_prompt_record(
    config: PromptRunnerConfig, manifest: dict[str, Any], prompt: PromptJob,
    reference: dict[str, Any], source: dict[str, Any], *, files: ReceiptFiles | None = None,
) -> dict[str, Any]:
    raw = source["final_output_raw"]
    return {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": (files.sha256 if files else _sha256_file)(config.run_directory / "manifest.json"),
        "wrapper_sha256": manifest["wrapper_sha256"],
        "job_key": "prompt:" + prompt.prompt_id, "stage": "prompt",
        "note_id": prompt.prompt_id, "takeaway_index": None, "attempt": 1,
        "status": "succeeded", "execution": "cross_run_reuse",
        "request_sha256": _prompt_request_key(manifest, prompt.text),
        "prompt_sha256": _sha256_bytes(prompt.text.encode("utf-8")),
        "response_schema_sha256": manifest.get("response_schema_sha256"),
        "provider": config.provider, "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "response": raw, "final_output_raw": raw,
        "final_output_sha256": _sha256_bytes(raw.encode("utf-8")),
        "usage": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
        "http_attempts": 0, "error": None, "reused_from": reference,
    }


class RequestHistory:
    """Index saved prompt receipts once per batch; serialize only identical requests."""

    def __init__(self, root: Path, *, exclude_run: Path | None = None, discover=True,
                 request_keys: dict[str, str] | None = None):
        # Optional current request identities mapped to their prompt digests.
        # Receipt hashes only select discovery candidates; manifests and receipt
        # provenance still establish their identities before reuse.
        self.directory = root / ".model-request-history"
        self.files = ReceiptFiles(manifest_path=exclude_run / 'manifest.json' if exclude_run else None)
        self.sources: dict[str, list[dict[str, Any]]] = {}
        if not discover or request_keys == {}:
            return
        prompt_hashes = set(request_keys.values()) if request_keys is not None else None
        ignored = {".git", "node_modules", "target", ".direnv", ".venv", "__pycache__",
                   self.directory.name}
        for directory, children, files in os.walk(root):
            children[:] = sorted(c for c in children if c not in ignored)
            if Path(directory) == exclude_run:
                continue
            if "manifest.json" not in files or "results.jsonl" not in files:
                continue
            path = Path(directory) / "results.jsonl"
            try:
                # Discovery needs only compact receipt references. Do not keep
                # every historical rendered prompt in each running batch.
                discovery_files = ReceiptFiles()
                latest = {}
                for number, line in enumerate(discovery_files.lines(path), 1):
                    # Another run may still be appending its last record.
                    if not line.endswith(b"\n"):
                        continue
                    record = json.loads(line)
                    candidate = ((record.get('note_id'), record.get('manifest_sha256'))
                        if record.get('status') == 'succeeded' and record.get('execution') is None
                        and record.get('stage') == 'prompt' and record.get('model_result') else None)
                    if (candidate is not None and request_keys is not None
                            and record.get('request_sha256') is not None
                            and record.get('prompt_sha256') is not None
                            and record['request_sha256'] not in request_keys
                            and record['prompt_sha256'] not in prompt_hashes):
                        candidate = None
                    # Failed, reused and nonmatching later records must replace
                    # any earlier original success for the same job.
                    latest[record["job_key"]] = (number, _sha256_bytes(line), candidate)
                if not any(candidate for _, _, candidate in latest.values()):
                    continue
                manifest = discovery_files.json(path.parent / "manifest.json")
                if manifest.get("mode") not in {"prompt", "mask_generation"}:
                    continue
                prompts = {p["prompt_id"]: p["text"] for p in manifest["prompts"]}
                for number, digest, candidate in latest.values():
                    if candidate is None:
                        continue
                    note_id, manifest_sha256 = candidate
                    key = _prompt_request_key(manifest, prompts[note_id])
                    if request_keys is not None and key not in request_keys:
                        continue
                    self.sources.setdefault(key, []).append({
                        "results_path": str(path.resolve()), "result_line": number,
                        "result_sha256": digest,
                        "manifest_sha256": manifest_sha256,
                    })
            except (OSError, ValueError, KeyError, TypeError, AttributeError, RunnerError):
                # Unrelated legacy or incomplete runs are not reusable evidence.
                continue

    @contextmanager
    def lock(self, key: str):
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / (key + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def matches(self, key: str):
        path = self.directory / (key + ".json")
        references = ([_load_json(path, "request history reference")] if path.exists() else [])
        references += self.sources.get(key, [])
        seen = set()
        for reference in references:
            identity = _canonical_json_bytes(reference)
            if identity not in seen:
                seen.add(identity)
                yield reference, _read_request_source(reference, key, files=self.files)

    def remember(self, key: str, path: Path, record: dict[str, Any]) -> None:
        expected = _canonical_json_bytes(record)
        with RESULTS_WRITE_LOCK:
            number = self.files.first_line(path, expected)
        reference = {"results_path": str(path.resolve()), "result_line": number,
                     "result_sha256": _sha256_bytes(expected),
                     "manifest_sha256": record["manifest_sha256"]}
        _atomic_write(self.directory / (key + ".json"), _canonical_json_bytes(reference))


def _run_job(
    *,
    config: RunnerConfig | SubjectRunnerConfig | PromptRunnerConfig,
    manifest_sha256: str,
    runner_version: str,
    stage_name: str,
    stage: StageConfig | PromptRunnerConfig,
    note: Note,
    takeaway_index: int | None,
    prompt: str,
    schema: dict[str, object] | None,
    results_path: Path,
    latest: dict[str, dict[str, Any]],
    retry_failed: bool,
    execution_wrapper_sha256: str,
    retry_directive: RetryDirective | None = None,
    new_job_budget: NewJobBudget | None = None,
    reuse_source: LoadedReuseSource | None = None,
    receipt_files: ReceiptFiles | None = None,
) -> list[str] | int | str:
    receipt_files = receipt_files or ReceiptFiles()
    job_key = _job_key(stage_name, note.note_id, takeaway_index)
    previous = latest.get(job_key)
    prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
    retry_reason: str | None = None
    if previous is not None and previous["status"] == "succeeded":
        if (
            previous.get("stage") != stage_name
            or previous.get("note_id") != note.note_id
            or previous.get("takeaway_index") != takeaway_index
        ):
            raise RunnerError("saved receipt job identity differs")
        _validate_cached_reuse_record(
            previous,
            reuse_source,
            job_key=job_key,
            stage_name=stage_name,
            stage=stage,
            note=note,
            takeaway_index=takeaway_index,
            prompt_sha256=prompt_sha256,
            schema=schema,
        )
        if previous.get("execution") != "reused":
            saved_prompt = (
                prompt
                if previous.get("prompt_sha256") == prompt_sha256
                else _wire_prompt(
                    _expect_object(previous.get("model_result"), "saved model result"),
                    config.provider,
                )
            )
            validate_model_receipt(
                config,
                stage,
                saved_prompt,
                schema,
                _expect_object(
                    receipt_files.json(config.run_directory / "manifest.json"),
                    "run manifest",
                ),
                previous, files=receipt_files,
            )
            raw_response = (
                previous["final_output_raw"]
                if stage_name == "prompt"
                else _validate_response(
                    stage_name,
                    json.loads(previous["final_output_raw"]),
                    note.text,
                    stage.prompt_version,
                )
            )
            if not _same_json(previous.get("response"), raw_response):
                raise RunnerError(
                    "saved receipt response differs from raw model output"
                )
        if retry_directive is not None and retry_directive.job_key == job_key:
            retry_directive.matched = True
            retry_reason = retry_directive.reason
        elif previous.get("prompt_sha256") != prompt_sha256:
            if not retry_failed:
                if new_job_budget is not None and new_job_budget.exhausted:
                    raise NewJobLimitReached
                raise RunnerError(
                    f"cached result for job `{job_key}` has a rendered prompt changed "
                    "by an upstream retry; inspect the superseding output and rerun "
                    "with --retry-failed"
                )
            retry_reason = "upstream rendered prompt changed"
        else:
            if stage_name == "prompt":
                response = previous.get("response")
                if not isinstance(response, str) or not response.strip():
                    raise RunnerError(
                        f"saved response for job `{job_key}` is not nonempty text"
                    )
                return response
            return _validate_result_items(
                stage_name,
                previous.get("response"),
                note.text,
                stage.prompt_version,
            )
    if previous is not None and not retry_failed:
        if retry_reason is None:
            raise RunnerError(
                f"job `{job_key}` previously failed; inspect results.jsonl and "
                "rerun with --retry-failed to create a new audited attempt"
            )
    if previous is not None and retry_reason is None:
        retry_reason = "retry after failed attempt"
    if (
        previous is None
        and reuse_source is not None
        and isinstance(stage, StageConfig)
        and schema is not None
    ):
        reusable = _find_reusable_response(
            reuse_source,
            stage_name=stage_name,
            stage=stage,
            note=note,
            takeaway_index=takeaway_index,
            prompt_sha256=prompt_sha256,
            schema=schema,
        )
        if reusable is not None:
            response, reference = reusable
            _append_reused_result(
                source=reuse_source,
                manifest_sha256=manifest_sha256,
                execution_wrapper_sha256=execution_wrapper_sha256,
                stage_name=stage_name,
                stage=stage,
                note=note,
                takeaway_index=takeaway_index,
                prompt_sha256=prompt_sha256,
                schema=schema,
                response=response,
                reference=reference,
                results_path=results_path,
                latest=latest,
            )
            return response
    from experiment_runner import scheduling
    with ExitStack() as admission:
        # Native batches already hold a queue slot. Sequential compatibility
        # stages use that same host queue, never a separate concurrency pool.
        queue = None
        if scheduling.current_slot() is None:
            identity = _runner_identity(config)
            queue = scheduling.ExperimentQueue(config.run_directory, 1,
                scope_loader=_experiment_scope_loader(config, stage),
                endpoint_loader=_experiment_endpoint_loader(config, identity), worker_identity=identity)
            admission.callback(queue.close)
            admission.enter_context(queue.slot(job_key, config.timeout_seconds))
        admitted_queue = scheduling.active_queue() or queue
        if admitted_queue is not None:
            admitted_queue.check_worker(_runner_identity(config))
        if new_job_budget is not None:
            new_job_budget.claim(job_key)
        attempt = 1 if previous is None else previous["attempt"] + 1
        try:
            attempt_result = _invoke_model(config, stage, prompt, schema, receipt_files=receipt_files)
        except BaseException:
            if new_job_budget is not None and new_job_budget.spend is not None:
                new_job_budget.spend.settle(None)
            raise
        if queue is not None:
            queue.completed(failed=attempt_result['error'] is not None)
    error_message = attempt_result["error"]
    output_raw = attempt_result["final_output_raw"]
    response: list[str] | int | str | None = None
    if error_message is None:
        try:
            response = (
                output_raw
                if stage_name == "prompt"
                else _validate_response(
                    stage_name, json.loads(output_raw), note.text, stage.prompt_version
                )
            )
        except (json.JSONDecodeError, RunnerError) as error:
            error_message = str(error)

    record: dict[str, object] = {
        "schema_version": 2 if stage_name in {"subject", "claim"} else SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "job_key": job_key,
        "stage": stage_name,
        "note_id": note.note_id,
        "attempt": attempt,
        "status": "failed" if error_message is not None else "succeeded",
        "model": stage.model,
        "reasoning_effort": stage.reasoning_effort,
        "runner_version": runner_version,
        "wrapper_sha256": execution_wrapper_sha256,
        "prompt_sha256": prompt_sha256,
        "response_schema_sha256": (
            None
            if schema is None
            else _sha256_bytes(
                _ordered_json_bytes(schema)
                if stage_name == "prompt"
                else _canonical_json_bytes(schema)
            )
        ),
        **attempt_result,
        "response": response,
        "error": error_message,
    }
    if stage_name != "subject":
        record["takeaway_index"] = takeaway_index
    if stage_name == "prompt":
        record["request_sha256"] = _prompt_request_key(
            receipt_files.json(config.run_directory / "manifest.json"), prompt
        )
    if retry_reason is not None:
        record["retry_reason"] = retry_reason
    try:
        _append_json_line(results_path, record,
                          receipt_files=receipt_files if stage_name == 'prompt' else None)
        latest[job_key] = record
    finally:
        if new_job_budget is not None and new_job_budget.spend is not None:
            new_job_budget.spend.settle(record)
    if error_message is not None:
        raise RunnerError(error_message)
    if response is None:
        raise AssertionError("successful job is missing a response")
    return response


def _takeaway_input(text: str) -> str:
    return NIP19_IDENTIFIER.sub("", text)


def _render_takeaway_prompt(prompt: str, note: Note) -> str:
    return f"{prompt}\n\n<post>\n{_takeaway_input(note.text)}\n</post>"


def _render_subject_prompt(prompt: str, note: Note) -> str:
    return f"{prompt}\n\n<post>\n{_takeaway_input(note.text)}\n</post>"


def _render_prominence_prompt(prompt: str, note: Note, takeaway: str) -> str:
    return (
        f"{prompt}\n\n<complete_post>\n{_takeaway_input(note.text)}"
        f"\n</complete_post>\n\n<takeaway>\n{takeaway}\n</takeaway>"
    )


def _render_mask_prompt(prompt: str, note: Note, takeaway: str) -> str:
    replacements = {"{POST}": note.text, "{TAKEAWAY}": takeaway}
    return re.sub(
        r"\{POST\}|\{TAKEAWAY\}",
        lambda match: replacements[match.group(0)],
        prompt,
    )


def _find_reusable_response(
    source: LoadedReuseSource,
    *,
    stage_name: str,
    stage: StageConfig,
    note: Note,
    takeaway_index: int | None,
    prompt_sha256: str,
    schema: dict[str, object],
) -> tuple[list[str] | int, SourceResultReference] | None:
    job_key = _job_key(stage_name, note.note_id, takeaway_index)
    reusable = source.jobs.get(job_key)
    if reusable is None or reusable.note_text != note.text:
        return None
    source_stage = source.stages[stage_name]
    target_schema_sha256 = _sha256_bytes(_canonical_json_bytes(schema))
    if (
        source_stage.prompt_version != stage.prompt_version
        or source_stage.model != stage.model
        or source_stage.reasoning_effort != stage.reasoning_effort
        or source_stage.response_schema_sha256 != target_schema_sha256
        or source_stage.prompt_sha256 != stage.prompt_sha256
        or reusable.rendered_prompt_sha256 != prompt_sha256
    ):
        return None
    response = _validate_result_items(
        stage_name,
        reusable.response,
        note.text,
        stage.prompt_version,
    )
    return response, reusable.reference


def _append_reused_result(
    *,
    source: LoadedReuseSource,
    manifest_sha256: str,
    execution_wrapper_sha256: str,
    stage_name: str,
    stage: StageConfig,
    note: Note,
    takeaway_index: int | None,
    prompt_sha256: str,
    schema: dict[str, object],
    response: list[str] | int,
    reference: SourceResultReference,
    results_path: Path,
    latest: dict[str, dict[str, Any]],
) -> None:
    job_key = _job_key(stage_name, note.note_id, takeaway_index)
    empty_sha256 = _sha256_bytes(b"")
    record: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "job_key": job_key,
        "stage": stage_name,
        "note_id": note.note_id,
        "takeaway_index": takeaway_index,
        "attempt": 1,
        "status": "succeeded",
        "execution": "reused",
        "model": stage.model,
        "reasoning_effort": stage.reasoning_effort,
        "runner_version": None,
        "wrapper_sha256": execution_wrapper_sha256,
        "prompt_sha256": prompt_sha256,
        "response_schema_sha256": _sha256_bytes(_canonical_json_bytes(schema)),
        "argv": [],
        "exit_code": None,
        "elapsed_seconds": 0.0,
        "model_result": None,
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
        },
        "stdout_json": "",
        "stdout_sha256": empty_sha256,
        "stderr": "",
        "stderr_sha256": empty_sha256,
        "final_output_raw": None,
        "final_output_sha256": None,
        "response": response,
        "error": None,
        "reused_from": {
            "input_sha256": source.config.input_sha256,
            "manifest_sha256": source.config.manifest_sha256,
            "results_sha256": source.config.results_sha256,
            "output_sha256": source.config.output_sha256,
            "result_line": reference.line,
            "result_sha256": reference.sha256,
            "result_attempt": reference.attempt,
        },
    }
    _append_json_line(results_path, record)
    latest[job_key] = record


def _validate_cached_reuse_record(
    record: dict[str, Any],
    source: LoadedReuseSource | None,
    *,
    job_key: str,
    stage_name: str,
    stage: StageConfig | PromptRunnerConfig,
    note: Note,
    takeaway_index: int | None,
    prompt_sha256: str,
    schema: dict[str, object] | None,
) -> None:
    execution = record.get("execution")
    if source is None:
        if execution is None:
            return
        raise RunnerError(
            f"cached result for job `{job_key}` has invalid reuse provenance"
        )
    if not isinstance(stage, StageConfig) or schema is None:
        raise RunnerError(f"cached result for job `{job_key}` has invalid reuse stage")
    reusable_response = _find_reusable_response(
        source,
        stage_name=stage_name,
        stage=stage,
        note=note,
        takeaway_index=takeaway_index,
        prompt_sha256=prompt_sha256,
        schema=schema,
    )
    if execution is None:
        if reusable_response is not None and record.get("attempt") == 1:
            raise RunnerError(
                f"cached result for job `{job_key}` is missing required reuse provenance"
            )
        return
    if execution != "reused" or reusable_response is None:
        raise RunnerError(
            f"cached result for job `{job_key}` has invalid reuse provenance"
        )
    reusable = source.jobs[job_key]
    reference = reusable.reference
    expected_provenance = {
        "input_sha256": source.config.input_sha256,
        "manifest_sha256": source.config.manifest_sha256,
        "results_sha256": source.config.results_sha256,
        "output_sha256": source.config.output_sha256,
        "result_line": reference.line,
        "result_sha256": reference.sha256,
        "result_attempt": reference.attempt,
    }
    empty_sha256 = _sha256_bytes(b"")
    expected_fields: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "job_key": job_key,
        "stage": stage_name,
        "note_id": note.note_id,
        "takeaway_index": takeaway_index,
        "attempt": 1,
        "status": "succeeded",
        "execution": "reused",
        "model": stage.model,
        "reasoning_effort": stage.reasoning_effort,
        "runner_version": None,
        "prompt_sha256": prompt_sha256,
        "response_schema_sha256": _sha256_bytes(_canonical_json_bytes(schema)),
        "argv": [],
        "exit_code": None,
        "elapsed_seconds": 0.0,
        "model_result": None,
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
        },
        "stdout_json": "",
        "stdout_sha256": empty_sha256,
        "stderr": "",
        "stderr_sha256": empty_sha256,
        "final_output_raw": None,
        "final_output_sha256": None,
        "response": reusable.response,
        "error": None,
        "reused_from": expected_provenance,
    }
    if any(record.get(field) != value for field, value in expected_fields.items()):
        raise RunnerError(
            f"cached result for job `{job_key}` does not match its reuse source"
        )


def _execute_pipeline_jobs(
    *,
    config: RunnerConfig,
    notes: tuple[Note, ...],
    manifest_sha256: str,
    runner_version: str,
    results_path: Path,
    latest: dict[str, dict[str, Any]],
    retry_failed: bool,
    execution_wrapper_sha256: str,
    retry_directive: RetryDirective | None,
    new_job_budget: NewJobBudget,
    reuse_source: LoadedReuseSource | None,
) -> list[dict[str, object]]:
    output_notes: list[dict[str, object]] = []

    for note in notes:
        takeaways = _run_job(
            config=config,
            manifest_sha256=manifest_sha256,
            runner_version=runner_version,
            stage_name="takeaway",
            stage=config.takeaway,
            note=note,
            takeaway_index=None,
            prompt=_render_takeaway_prompt(config.takeaway.prompt, note),
            schema=TAKEAWAY_SCHEMA,
            results_path=results_path,
            latest=latest,
            retry_failed=retry_failed,
            execution_wrapper_sha256=execution_wrapper_sha256,
            retry_directive=retry_directive,
            new_job_budget=new_job_budget,
            reuse_source=reuse_source,
        )
        if not isinstance(takeaways, list) or not all(
            isinstance(takeaway, str) for takeaway in takeaways
        ):
            raise AssertionError("takeaway job returned an invalid response")
        output_takeaways: list[dict[str, object]] = []
        for index, takeaway in enumerate(takeaways):
            prominence: int | None = None
            if config.prominence is not None:
                prominence_result = _run_job(
                    config=config,
                    manifest_sha256=manifest_sha256,
                    runner_version=runner_version,
                    stage_name="prominence",
                    stage=config.prominence,
                    note=note,
                    takeaway_index=index,
                    prompt=_render_prominence_prompt(
                        config.prominence.prompt,
                        note,
                        takeaway,
                    ),
                    schema=PROMINENCE_SCHEMA,
                    results_path=results_path,
                    latest=latest,
                    retry_failed=retry_failed,
                    execution_wrapper_sha256=execution_wrapper_sha256,
                    retry_directive=retry_directive,
                    new_job_budget=new_job_budget,
                    reuse_source=reuse_source,
                )
                if isinstance(prominence_result, bool) or not isinstance(
                    prominence_result, int
                ):
                    raise AssertionError("prominence job returned an invalid response")
                prominence = prominence_result
            fragments = _run_job(
                config=config,
                manifest_sha256=manifest_sha256,
                runner_version=runner_version,
                stage_name="mask",
                stage=config.mask,
                note=note,
                takeaway_index=index,
                prompt=_render_mask_prompt(config.mask.prompt, note, takeaway),
                schema=MASK_SCHEMA,
                results_path=results_path,
                latest=latest,
                retry_failed=retry_failed,
                execution_wrapper_sha256=execution_wrapper_sha256,
                retry_directive=retry_directive,
                new_job_budget=new_job_budget,
                reuse_source=reuse_source,
            )
            output_takeaway: dict[str, object] = {
                "text": takeaway,
                "fragments": fragments,
            }
            if prominence is not None:
                output_takeaway["prominence"] = prominence
            output_takeaways.append(output_takeaway)
        output_notes.append({"note_id": note.note_id, "takeaways": output_takeaways})
        new_job_budget.completed_notes += 1
        new_job_budget.completed_takeaways += len(output_takeaways)

    return output_notes


def run(
    config_path: Path,
    *,
    retry_failed: bool,
    max_new_jobs: int | None = None,
    retry_job: str | None = None,
    retry_reason: str | None = None,
    runner_upgrade_reason: str | None = None,
) -> PipelineRunOutcome:
    if max_new_jobs is not None and max_new_jobs < 1:
        raise RunnerError("--max-new-jobs must be at least 1")
    config = RunnerConfig.load(config_path)
    _validate_auth_file(config.auth_file)
    notes = _load_notes(config.input_path)
    reuse_source = (
        None
        if config.reuse_source is None
        else _load_reuse_source(config.reuse_source, notes, config.provider)
    )
    runner_version = _runner_version(config)
    manifest = _build_manifest(config, runner_version)
    prepared_manifest = _prepare_run_directory(
        config,
        manifest,
        runner_upgrade_reason=runner_upgrade_reason,
    )
    results_path = config.run_directory / "results.jsonl"
    latest = _load_latest_results(
        results_path,
        prepared_manifest.manifest_sha256,
        prepared_manifest.provenance,
    )
    execution_provenance = _append_runner_upgrade(
        config.run_directory,
        prepared_manifest,
    )
    manifest_sha256 = prepared_manifest.manifest_sha256
    retry_directive: RetryDirective | None = None
    if retry_job is not None:
        previous = latest.get(retry_job)
        if previous is None:
            raise RunnerError(f"--retry-job `{retry_job}` has no saved result")
        if previous.get("status") != "succeeded":
            raise RunnerError(
                f"--retry-job `{retry_job}` is not a successful result; use "
                "--retry-failed instead"
            )
        retry_directive = RetryDirective(retry_job, retry_reason or "")
    (config.run_directory / "output.json").unlink(missing_ok=True)
    new_job_budget = NewJobBudget(max_new_jobs)

    try:
        try:
            output_notes = _execute_pipeline_jobs(
                config=config,
                notes=notes,
                manifest_sha256=manifest_sha256,
                runner_version=runner_version,
                results_path=results_path,
                latest=latest,
                retry_failed=retry_failed,
                execution_wrapper_sha256=execution_provenance.active_wrapper_sha256,
                retry_directive=retry_directive,
                new_job_budget=new_job_budget,
                reuse_source=reuse_source,
            )
        except NewJobLimitReached:
            if retry_directive is not None and not retry_directive.matched:
                raise RunnerError(
                    f"--retry-job `{retry_directive.job_key}` was not reached before "
                    "--max-new-jobs"
                )
            return PipelineRunOutcome(
                complete=False,
                new_jobs=new_job_budget.used,
                completed_notes=new_job_budget.completed_notes,
                completed_takeaways=new_job_budget.completed_takeaways,
                output=None,
            )

        if retry_directive is not None and not retry_directive.matched:
            raise RunnerError(
                f"--retry-job `{retry_directive.job_key}` is not part of the current pipeline"
            )
        output: dict[str, object] = {
            "schema_version": config.schema_version,
            "manifest_sha256": manifest_sha256,
            "notes": output_notes,
        }
        _atomic_write(
            config.run_directory / "output.json", _canonical_json_bytes(output)
        )
        return PipelineRunOutcome(
            complete=True,
            new_jobs=new_job_budget.used,
            completed_notes=new_job_budget.completed_notes,
            completed_takeaways=new_job_budget.completed_takeaways,
            output=output,
        )
    finally:
        if config.reuse_source is not None:
            _verify_reuse_source_hashes(config.reuse_source)


def _execute_subject_jobs(
    *,
    config: SubjectRunnerConfig,
    notes: tuple[Note, ...],
    manifest_sha256: str,
    runner_version: str,
    results_path: Path,
    latest: dict[str, dict[str, Any]],
    retry_failed: bool,
    execution_wrapper_sha256: str,
    new_job_budget: NewJobBudget,
) -> list[dict[str, object]]:
    output_notes: list[dict[str, object]] = []
    for note in notes:
        subjects = _run_job(
            config=config,
            manifest_sha256=manifest_sha256,
            runner_version=runner_version,
            stage_name="subject",
            stage=config.subject,
            note=note,
            takeaway_index=None,
            prompt=_render_subject_prompt(config.subject.prompt, note),
            schema=SUBJECT_SCHEMA,
            results_path=results_path,
            latest=latest,
            retry_failed=retry_failed,
            execution_wrapper_sha256=execution_wrapper_sha256,
            new_job_budget=new_job_budget,
        )
        if not isinstance(subjects, list) or not all(
            isinstance(subject, str) for subject in subjects
        ):
            raise AssertionError("subject job returned an invalid response")
        output_notes.append(
            {
                "note_id": note.note_id,
                "text": note.text,
                "subjects": [
                    {"source_index": index, "text": subject}
                    for index, subject in enumerate(subjects)
                ],
            }
        )
        new_job_budget.completed_notes += 1
    return output_notes


def run_subjects(
    config_path: Path,
    *,
    retry_failed: bool,
    max_new_jobs: int | None = None,
) -> SubjectRunOutcome:
    if max_new_jobs is not None and max_new_jobs < 1:
        raise RunnerError("--max-new-jobs must be at least 1")
    config = SubjectRunnerConfig.load(config_path)
    _validate_auth_file(config.auth_file)
    notes = _load_notes(config.input_path)
    runner_version = _runner_version(config)
    manifest = _build_subject_manifest(config, runner_version)
    prepared = _prepare_subject_run_directory(config, manifest)
    manifest_sha256 = prepared.manifest_sha256
    results_path = config.run_directory / "results.jsonl"
    latest = _load_latest_results(
        results_path,
        manifest_sha256,
        prepared.provenance,
    )
    (config.run_directory / "output.json").unlink(missing_ok=True)
    new_job_budget = NewJobBudget(max_new_jobs)

    try:
        output_notes = _execute_subject_jobs(
            config=config,
            notes=notes,
            manifest_sha256=manifest_sha256,
            runner_version=runner_version,
            results_path=results_path,
            latest=latest,
            retry_failed=retry_failed,
            execution_wrapper_sha256=prepared.provenance.active_wrapper_sha256,
            new_job_budget=new_job_budget,
        )
    except NewJobLimitReached:
        return SubjectRunOutcome(
            complete=False,
            new_jobs=new_job_budget.used,
            completed_notes=new_job_budget.completed_notes,
            output=None,
        )

    output: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "notes": output_notes,
    }
    _atomic_write(
        config.run_directory / "output.json",
        _canonical_json_bytes(output),
    )
    return SubjectRunOutcome(
        complete=True,
        new_jobs=new_job_budget.used,
        completed_notes=new_job_budget.completed_notes,
        output=output,
    )


def revise_prompt(base_path: Path, destination: Path, request: object) -> str:
    """Create an offline, no-clobber revision from exact edits against a pinned base."""
    request = _expect_object(request, "revision")
    _expect_fields(request, "revision", {"base_sha256", "edits"})
    expected_hash = _expect_sha256(request["base_sha256"], "revision.base_sha256")
    edits = request["edits"]
    if not isinstance(edits, list) or not edits:
        raise RunnerError("revision.edits must be a nonempty array")
    base_path = base_path.expanduser().resolve()
    destination = destination.expanduser().absolute()
    receipt_path = destination.with_name(destination.name + ".revision.json")
    try:
        original = base_path.read_bytes()
        original_text = original.decode("utf-8")
    except (OSError, UnicodeError) as error:
        raise RunnerError(f"could not read baseline prompt: {error}") from error
    if _sha256_bytes(original) != expected_hash:
        raise RunnerError("baseline SHA-256 does not match revision.base_sha256")
    for path in (destination, receipt_path):
        if os.path.lexists(path):
            raise RunnerError(f"revision destination already exists: {path}")

    replacements = []
    for index, raw_edit in enumerate(edits):
        name = f"revision.edits[{index}]"
        edit = _expect_object(raw_edit, name)
        _expect_fields(edit, name, {"old", "new"})
        old = _expect_string(edit["old"], f"{name}.old")
        new = edit["new"]
        if not isinstance(new, str) or "\x00" in new:
            raise RunnerError(f"{name}.new must be a string without NUL bytes")
        try:
            old_bytes, new_bytes = old.encode("utf-8"), new.encode("utf-8")
        except UnicodeError as error:
            raise RunnerError(f"{name} must contain valid UTF-8 text") from error
        start = original.find(old_bytes)
        if start < 0 or original.find(old_bytes, start + 1) >= 0:
            raise RunnerError(f"{name}.old must match the original prompt exactly once")
        if old_bytes == new_bytes:
            raise RunnerError(f"{name} leaves the prompt unchanged")
        replacements.append((start, start + len(old_bytes), new_bytes))
    parts, cursor = [], 0
    for start, end, replacement in sorted(replacements):
        if start < cursor:
            raise RunnerError("revision edits overlap in the original prompt")
        parts.extend((original[cursor:start], replacement))
        cursor = end
    parts.append(original[cursor:])
    revised = b"".join(parts)
    if revised == original:
        raise RunnerError("revision leaves the prompt unchanged")

    receipt = {
        "schema_version": 1,
        "base_path": str(base_path),
        "base_sha256": expected_hash,
        "prompt_sha256": _sha256_bytes(revised),
        "edits": edits,
    }
    diff_lines = difflib.unified_diff(
        original_text.splitlines(keepends=True),
        revised.decode("utf-8").splitlines(keepends=True),
        fromfile=str(base_path), tofile=str(destination),
        n=max(len(original_text.splitlines()), len(revised.splitlines())),
    )
    diff = "".join(
        line if line.endswith("\n") else line + "\n\\ No newline at end of file\n"
        for line in diff_lines
    )
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".prompt-revision-", dir=destination.parent) as temporary:
            staged_receipt = Path(temporary) / "receipt"
            staged_prompt = Path(temporary) / "prompt"
            _atomic_write(staged_receipt, _canonical_json_bytes(receipt))
            _atomic_write(staged_prompt, revised)
            published = []
            try:
                # Publish complete files without replacing existing paths. The receipt
                # claims this destination first; the prompt becomes visible last.
                for source, target in ((staged_receipt, receipt_path), (staged_prompt, destination)):
                    os.link(source, target)
                    published.append((source, target))
            except OSError:
                for source, target in reversed(published):
                    try:
                        if os.path.samestat(source.stat(), target.lstat()):
                            target.unlink()
                    except FileNotFoundError:
                        pass
                raise
    except OSError as error:
        raise RunnerError(f"could not create prompt revision without overwriting: {error}") from error
    return diff


def run_prompts(
    config_path: Path, *, retry_failed: bool, repeat: bool = False,
    history_root: Path | None = None,
) -> dict[str, object]:
    config = PromptRunnerConfig.load(config_path)
    _validate_auth_file(config.auth_file)
    runner_version = _runner_version(config)
    manifest = _build_prompt_manifest(config, runner_version)
    _request_history_options(config, manifest, repeat=repeat, history_root=history_root)
    return _execute_prompt_batch(config, manifest, retry_failed=retry_failed,
                                 concurrency=config.concurrency)


_PROVIDER_TRANSIENT_CODES = frozenset({
    'server_error', 'server_is_overloaded', 'rate_limit_exceeded', 'slow_down',
    'usage_limit_reached', 'websocket_connection_limit_reached',
    'service_unavailable_error', 'rate_limit_error', 'overloaded_error',
})
_PROVIDER_FATAL_CODES = frozenset({
    'insufficient_quota', 'invalid_api_key', 'authentication_error', 'authentication',
    'permission_denied', 'payment_required', 'usage_not_included',
    'billing_hard_limit_reached', 'billing_not_active',
    'project_spend_limit_exceeded', 'organization_usage_limit_exceeded',
})
_PROVIDER_REQUEST_ERRORS = frozenset({
    'invalid_request', 'invalid_request_error', 'invalid_prompt', 'not_found',
    'unsupported_parameter', 'unsupported_value', 'invalid_model', 'model_not_found',
    'precondition_failed', 'payload_too_large', 'unprocessable',
    'context_length_exceeded', 'max_tokens_exceeded', 'token_limit_exceeded',
    'string_too_long', 'content_policy_violation', 'refusal',
    'invalid_image', 'image_too_large', 'image_too_small', 'unsupported_image_format',
    'image_not_found',
})
_TRANSPORT_TRANSIENT_CODES = frozenset({
    'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EAI_AGAIN',
    'ENETUNREACH', 'EHOSTUNREACH', 'UND_ERR_SOCKET', 'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT',
})


def prompt_failure_kind(record: dict[str, Any]) -> str:
    """Retry only explicit transient provider responses, never inferred outages."""
    result = record.get("model_result") or {}
    if not isinstance(result, dict):
        return "unknown"
    metadata = result.get("response_metadata") or {}
    if not isinstance(metadata, dict):
        return "unknown"
    # A local abort or successful transport followed by validation failure takes
    # precedence over any provider metadata that happened to arrive earlier.
    if (metadata.get("abort_reason") or record.get("exit_code") in (None, 0)
            or result.get("stop_reason") in ("stop", "length", "aborted", "toolUse")):
        return "local"
    proxy = metadata.get("proxy_status") or {}
    if (metadata.get("terminal_event") == "response.completed"
            or isinstance(proxy, dict) and (proxy.get("terminal_event") == "completed"
                                            or proxy.get("state") == "complete")):
        return "local"
    # Proxy cleanup can cancel its context after an upstream terminal failure.
    # Either receipt layer can record the upstream failed event. It still needs
    # the provider-code checks below; cancellation without that event is local.
    if (metadata.get('terminal_event') != 'response.failed' and isinstance(proxy, dict)
            and proxy.get('terminal_event') != 'failed'
            and (proxy.get('state') in ('canceled', 'deadline_exceeded')
                 or proxy.get('request_context') in ('canceled', 'deadline_exceeded'))):
        return "local"
    error = metadata.get("error") or {}
    code = error.get("code") if isinstance(error, dict) else None
    if isinstance(code, str) and code.isdecimal():
        code = int(code)
    status = metadata.get("http_status")
    if isinstance(status, str) and status.isdecimal():
        status = int(status)
    labels = {error.get(key) for key in ('code', 'type', 'error_type')
              if isinstance(error, dict) and isinstance(error.get(key), str)}
    # Provider-owned quota/auth and request errors override a generic wrapper
    # status. Retrying cannot repair the request or replenish paid credit.
    if labels & _PROVIDER_FATAL_CODES:
        return "provider_fatal"
    if status in (401, 403, 404) or code in (401, 403, 404):
        return "provider_fatal"
    if status == 402 or code == 402:
        # OpenRouter documents one temporary 402: outstanding in-flight spending
        # with an explicit wait deadline. Ordinary exhausted credit stays fatal.
        if (result.get('provider') == 'openrouter' and isinstance(error, dict)
                and error.get('limit_source') == 'openrouter_in_flight_budget'
                and metadata.get('should_retry') is not False
                and any(metadata.get(key) is not None for key in ('retry_after', 'retry_after_ms'))
                and _provider_retry_after_delay(record) is not None):
            return "provider_transient"
        return "provider_fatal"
    # A generic invalid_request_error envelope can also wrap rate limits and
    # server failures. Specific request codes still stop; a generic label alone
    # must not hide the actual HTTP 429/503.
    if (labels & (_PROVIDER_REQUEST_ERRORS - {'invalid_request_error'})
            or code == 'invalid_request_error' and not (type(status) is int
                and (status in (408, 409, 425, 429) or 500 <= status <= 599))
            or isinstance(error, dict) and error.get('error_type') == 'unmapped'):
        return "unknown"
    if metadata.get('should_retry') is False:
        return 'unknown'
    if metadata.get('should_retry') is True and result.get('stop_reason') == 'error':
        return 'provider_transient'
    # An error code inside an HTTP-200/101 stream is separate from the transport
    # status. Conversely, a symbolic body code must not hide an HTTP 503.
    if type(status) is int and 400 <= status < 500 and status not in (408, 409, 425, 429):
        return "unknown"
    if any(type(value) is int and (value in (408, 409, 425, 429) or 500 <= value <= 599)
           for value in (status, code)):
        return "provider_transient"
    if (result.get("stop_reason") == "error" and
            (labels & _PROVIDER_TRANSIENT_CODES or result.get('provider') == 'openrouter'
             and isinstance(error, dict) and error.get('error_type') in (
                 'provider_overloaded', 'provider_unavailable', 'server', 'timeout'))):
        return "provider_transient"
    if (result.get("stop_reason") == "error"
            and metadata.get("terminal_event") not in ("response.failed", "response.incomplete")
            and isinstance(proxy, dict) and proxy.get("state") == "unknown"
            and proxy.get("request_context") == "active"
            and proxy.get("error_phase") in ("headers", "body")
            and not proxy.get("terminal_event")):
        kind = proxy.get("read_error_kind")
        if kind in ("unexpected_eof", "connection_reset", "connection_closed",
                    "broken_pipe", "timeout", "network_error"):
            return "provider_transient"
        if (kind in ("http2_goaway", "http2_stream_error", "http2_connection_error")
                and proxy.get("read_error_code") in (
                    "no_error", "internal_error", "refused_stream", "cancel", "enhance_your_calm",
                    "settings_timeout", "connect_error")):
            return "provider_transient"
    if result.get("provider") == "openai-codex" and result.get("stop_reason") == "error":
        close = metadata.get("websocket_close") or {}
        transport = metadata.get("transport_error") or {}
        if (status == 101 and metadata.get('terminal_event') not in (
                'response.failed', 'response.incomplete', 'error')
                and isinstance(close, dict) and close.get("code") in (1000, 1001, 1006, 1011, 1012, 1013, 1014)
                and isinstance(transport, dict) and transport.get("phase") == "body"):
            return "provider_transient"
    # The native fetch path records bounded, structured cause codes. A confirmed
    # connection failure is retryable even if the proxy supplied no receipt;
    # arbitrary exception messages and certificate/configuration errors are not.
    transport = metadata.get('transport_error') or {}
    if (result.get('stop_reason') == 'error'
            and metadata.get('terminal_event') not in ('response.failed', 'response.incomplete', 'error')
            and isinstance(transport, dict) and transport.get('phase') in ('headers', 'body')):
        for _ in range(3):
            if isinstance(transport.get('code'), str) and transport['code'] in _TRANSPORT_TRANSIENT_CODES:
                return 'provider_transient'
            transport = transport.get('cause')
            if not isinstance(transport, dict):
                break
    # Codex can fail inside an HTTP-200 stream without a structured error code.
    # Recognize only its explicit processing-error retry message, not arbitrary
    # error prose, transport failures, or model-generated output.
    message = result.get("error")
    if (status == 200 and result.get("provider") == "openai-codex"
            and result.get("stop_reason") == "error" and isinstance(message, str)
            and message.startswith("Codex error: An error occurred while processing your request. "
                                   "You can retry your request, or contact us")):
        return "provider_transient"
    return "unknown"


def provider_cooldown_scope(record: dict[str, Any]) -> str | None:
    """Shared pressure requires explicit rate, usage, or connection evidence."""
    kind = prompt_failure_kind(record)
    if kind != "provider_transient":
        return None
    result = record.get("model_result") or {}
    if not isinstance(result, dict):
        return None
    metadata = result.get("response_metadata") or {}
    if not isinstance(metadata, dict):
        return None
    error = metadata.get("error") or {}
    code = error.get("code") if isinstance(error, dict) else None
    pressure = {'rate_limit_exceeded', 'usage_limit_reached', 'slow_down',
                'websocket_connection_limit_reached', 'rate_limit_error'}
    if (metadata.get("http_status") in (429, "429") or code in (429, "429")
            or isinstance(error, dict) and any(isinstance(error.get(key), str)
                and error[key] in pressure for key in ('code', 'type', 'error_type'))):
        return "account"
    if (result.get('provider') == 'openrouter' and isinstance(error, dict)
            and error.get('limit_source') == 'openrouter_in_flight_budget'
            and (metadata.get('http_status') in (402, '402') or code in (402, '402'))):
        return "account"
    proxy = metadata.get("proxy_status") or {}
    if (kind == "provider_transient" and isinstance(proxy, dict)
            and proxy.get("state") == "unknown" and proxy.get("request_context") == "active"
            and proxy.get("error_phase") in ("headers", "body")
            and not proxy.get("terminal_event")):
        if proxy.get("read_error_kind") in (
                "http2_goaway", "http2_connection_error", "unexpected_eof", "connection_reset",
                "connection_closed", "broken_pipe", "network_error"):
            return "transport"
        if (proxy.get("read_error_kind") == "http2_stream_error"
                and proxy.get("read_error_code") == "enhance_your_calm"):
            return "account"
    return None


def provider_cooldown_required(record: dict[str, Any]) -> bool:
    return provider_cooldown_scope(record) is not None


def _provider_retry_after_delay(record: dict[str, Any]) -> float | None:
    metadata = (record.get("model_result") or {}).get("response_metadata", {})
    delays = []
    supplied = False
    for key in ('retry_after', 'retry_after_ms'):
        value = metadata.get(key)
        if value is None:
            continue
        supplied = True
        try:
            try:
                delay = float(value)
            except ValueError:
                if key == 'retry_after_ms':
                    continue
                delay = parsedate_to_datetime(value).timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            continue
        if math.isfinite(delay):
            delays.append(delay / 1000 if key == 'retry_after_ms' else delay)
    if delays:
        return max(2.0, *delays)
    return None if supplied else 2.0


def provider_retry_delay(record: dict[str, Any], *, keep_retrying: bool = False) -> float | None:
    """Every verified temporary failure retries; the old opt-in is compatible."""
    if prompt_failure_kind(record) != "provider_transient":
        return None
    # Bound before exponentiation, since saved lifetime counts can be arbitrarily
    # large. A malformed header cannot negate verified temporary failure evidence.
    backoff = min(60.0, 2.0 ** min(6, record["attempt"]))
    return max(backoff, _provider_retry_after_delay(record) or 0.0)


def _execute_prompt_batch(
    config: PromptRunnerConfig, manifest: dict[str, Any], *,
    retry_failed: bool, concurrency: int = 1,
    validate_output: Callable[[PromptJob, str], None] | None = None,
    max_new_attempts: int | None = None,
    runner_upgrade_reason: str | None = None,
    max_cost_usd: float | None = None,
    max_request_cost_usd: float | None = None,
    attempt_budget_path: Path | None = None,
    additional_attempt_budget_paths: Sequence[Path] = (),
    new_job_budget: NewJobBudget | None = None,
    before_new_attempt: Callable[[NewJobBudget], None] | None = None,
    request_queue=None,
    keep_retrying_provider_errors: bool = False,
    scheduling_group: str | None = None,
) -> dict[str, object]:
    from experiment_runner import scheduling
    # Historical concurrency stays in frozen plans; dispatch capacity is internal.
    concurrency = scheduling.LIMIT
    if additional_attempt_budget_paths and attempt_budget_path is None:
        raise RunnerError('additional attempt budgets require a primary attempt_budget_path')
    runner_version = manifest["runner_version"]
    proposed_manifest = {**manifest, "wrapper_sha256": _sha256_file(Path(__file__).resolve())}
    prepared_manifest = _prepare_manifest_directory(
        config.run_directory, proposed_manifest, runner_upgrade_reason=runner_upgrade_reason
    )
    worker_identity = _runner_identity(config)
    if request_queue is not None:
        request_queue.check_worker(worker_identity)
    compatibility.upgrade_worker(config.run_directory,
        _load_json(config.run_directory / 'manifest.json', 'run manifest'), worker_identity)
    manifest_sha256 = prepared_manifest.manifest_sha256
    schema_path = None
    if config.output_schema is not None:
        schema_path = config.run_directory / "prompt-output-schema.json"
        _atomic_write(schema_path, _ordered_json_bytes(config.output_schema))
    results_path = config.run_directory / "results.jsonl"
    latest = _load_latest_results(
        results_path,
        manifest_sha256,
        prepared_manifest.provenance,
    )
    provenance = _append_runner_upgrade(config.run_directory, prepared_manifest)
    if (max_cost_usd is None) != (max_request_cost_usd is None):
        raise RunnerError('both dollar limits are required together')
    spend = None
    if max_cost_usd is not None:
        if config.provider != 'openrouter':
            raise RunnerError('dollar budget requires provider-reported OpenRouter billing')
        records = [json.loads(line) for line in results_path.read_bytes().splitlines()] if results_path.exists() else []
        limits_path = config.run_directory/'spend-limits.jsonl'
        prior_limits = [json.loads(line) for line in limits_path.read_bytes().splitlines()] if limits_path.exists() else []
        if any(r.get('manifest_sha256') != manifest_sha256 for r in prior_limits):
            raise RunnerError('saved dollar limits belong to another manifest')
        # A smaller allowance on resume cannot discount an earlier unbilled call.
        prior_request = max([Decimal(str(max_request_cost_usd)),
                             *(Decimal(str(r['max_request_cost_usd'])) for r in prior_limits)])
        spend = PromptSpendBudget(max_cost_usd, max_request_cost_usd, records,
                                  prior_request_usd=prior_request,
                                  reconciled_costs=load_reconciled_costs(config.run_directory, records))
        _append_json_line(limits_path, dict(
            max_cost_usd=max_cost_usd, max_request_cost_usd=max_request_cost_usd,
            prior_spend_micro_usd=spend.spent, manifest_sha256=manifest_sha256,
            wrapper_sha256=provenance.active_wrapper_sha256))
    budget = new_job_budget or NewJobBudget(max_new_attempts, spend=spend,
                         persistent=PersistentAttemptBudget(
                             attempt_budget_path, tuple(additional_attempt_budget_paths))
                         if attempt_budget_path is not None else None)
    owned_queue = request_queue is None
    queue = request_queue or scheduling.ExperimentQueue(config.run_directory, len(config.prompts),
        scope_loader=_experiment_scope_loader(config, config),
        endpoint_loader=_experiment_endpoint_loader(config, worker_identity), worker_identity=worker_identity,
        scheduling_group=scheduling_group)
    if not owned_queue:
        queue.extend(len(config.prompts))
    (config.run_directory / "output.json").unlink(missing_ok=True)
    options = manifest["request_reuse"]
    history = RequestHistory(Path(options["history_root"]), exclude_run=config.run_directory,
        discover=not options['intentional_repeat'], request_keys={
            _prompt_request_key(manifest, prompt.text): _sha256_bytes(prompt.text.encode('utf-8'))
            for prompt in config.prompts if "prompt:" + prompt.prompt_id not in latest})
    # Parse frozen inputs before provider slots are held, then share the existing
    # operation cache across invocation checks and receipt request digests.
    history.files.json(config.run_directory / "manifest.json")
    mask_notes = {item["item_id"]: item["note"] for item in manifest.get("items", [])}

    def valid_for_reuse(prompt: PromptJob, raw: str) -> bool:
        try:
            if validate_output is not None:
                validate_output(prompt, raw)
            elif prompt.prompt_id in mask_notes:
                _map_mask_fragments(raw, mask_notes[prompt.prompt_id], manifest.get("mask_format", "fragments"))
        except (RunnerError, ValueError):
            return False
        return True

    def remember(key: str, prompt: PromptJob) -> None:
        record = latest["prompt:" + prompt.prompt_id]
        if not valid_for_reuse(prompt, record["final_output_raw"]):
            return
        history.remember(key, results_path, record)

    def execute(prompt: PromptJob) -> dict[str, str]:
        job_key = "prompt:" + prompt.prompt_id
        previous = latest.get(job_key)
        if previous is not None and previous.get("execution") == "cross_run_reuse":
            validate_prompt_receipt(replace(config, prompts=(prompt,)), manifest, previous, files=history.files)
            return {"prompt_id": prompt.prompt_id, "response": previous["response"]}
        arguments = dict(
            config=config,
            manifest_sha256=manifest_sha256,
            runner_version=runner_version,
            stage_name="prompt",
            stage=config,
            note=Note(note_id=prompt.prompt_id, text=""),
            takeaway_index=None,
            prompt=prompt.text,
            schema=config.output_schema,
            results_path=results_path,
            latest=latest,
            retry_failed=previous is not None and prompt_failure_kind(previous) == "provider_transient",
            new_job_budget=budget,
            execution_wrapper_sha256=provenance.active_wrapper_sha256,
            receipt_files=history.files,
        )
        def dispatch():
            if previous is not None and not arguments['retry_failed']:
                return _run_job(**arguments)
            with queue.activate():
                if before_new_attempt is not None:
                    before_new_attempt(budget)
                with queue.slot(('warmup:' if not owned_queue else '') + prompt.prompt_id, config.timeout_seconds):
                    return _run_job(**arguments)
        if previous is not None:
            response = dispatch()
        else:
            key = _prompt_request_key(manifest, prompt.text)
            if options["intentional_repeat"]:
                response = dispatch()
                with history.lock(key):
                    remember(key, prompt)
            else:
                with history.lock(key):
                    reusable = None
                    for reference, source in history.matches(key):
                        if not valid_for_reuse(prompt, source["final_output_raw"]):
                            continue
                        reusable = reference, source
                        break
                    if reusable is None:
                        response = dispatch()
                        remember(key, prompt)
                    else:
                        reference, source = reusable
                        record = _reused_prompt_record(config, manifest, prompt, reference, source, files=history.files)
                        record["wrapper_sha256"] = provenance.active_wrapper_sha256
                        _append_json_line(results_path, record, receipt_files=history.files)
                        latest[job_key] = record
                        response = record["response"]
        if not isinstance(response, str):
            raise AssertionError("successful prompt job returned structured output")
        return {"prompt_id": prompt.prompt_id, "response": response}

    ready = []  # Scheduled retries, ordered by their individual deadlines.
    fresh = deque()
    responses = {}
    failures = []
    fatal = False
    paused = False
    backoff_seconds = 0.0
    backoff_until = 0.0
    backoff_started = 0.0

    def backoff(record: dict[str, Any], now: float, submitted_at: float) -> None:
        nonlocal backoff_seconds, backoff_until, backoff_started
        # Already submitted requests share one outage. Only a newer request
        # establishes a new failure; an older one may carry a server deadline.
        if submitted_at >= backoff_started:
            backoff_seconds = min(60.0, max(2.0, backoff_seconds * 2))
            backoff_started = now
            backoff_until = max(backoff_until, now + max(
                backoff_seconds, _provider_retry_after_delay(record) or 0.0))
        elif any((record.get("model_result") or {}).get("response_metadata", {}).get(key) is not None
                 for key in ('retry_after', 'retry_after_ms')):
            delay = _provider_retry_after_delay(record)
            if delay is not None:
                backoff_until = max(backoff_until, now + delay)
        queue.cooldown(backoff_until)

    for index, prompt in enumerate(config.prompts):
        previous = latest.get("prompt:" + prompt.prompt_id)
        if previous is not None and previous["status"] == "failed":
            kind = prompt_failure_kind(previous)
            now = time.monotonic()
            # Resume retains the actual shared cooldown. An old receipt is not
            # evidence of a new outage starting when this coordinator restarts.
            delay = provider_retry_delay(previous, keep_retrying=keep_retrying_provider_errors)
            if delay is not None:
                retry_at = now + delay
                heapq.heappush(ready, (retry_at, index, prompt))
            else:
                failures.append(f"{prompt.prompt_id}: {previous.get('error')}")
                fatal |= kind == "provider_fatal"
                queue.completed(failed=True, warmup=not owned_queue)
        else:
            fresh.append((index, prompt))
    # A due retry takes priority; while it waits, fresh cases can use free slots.
    # Only shared-pressure evidence delays the batch. Every verified temporary
    # failure keeps retrying automatically within the existing budgets.
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            try:
                active = {}
                while active or (not fatal and (ready or fresh)):
                    try:
                        queue.check_pause()
                    except ExperimentPaused:
                        paused = fatal = True
                    while ((ready or fresh) and len(active) < concurrency and not fatal
                           and time.monotonic() >= backoff_until):
                        if ready and ready[0][0] <= time.monotonic():
                            _, index, prompt = heapq.heappop(ready)
                        elif fresh:
                            index, prompt = fresh.popleft()
                        else:
                            break
                        previous = latest.get("prompt:" + prompt.prompt_id)
                        submitted_at = time.monotonic()
                        active[pool.submit(execute, prompt)] = (index, prompt, previous, submitted_at)
                    now = time.monotonic()
                    launch_at = None
                    if not fatal and len(active) < concurrency:
                        if fresh or ready:
                            launch_at = max(backoff_until, now if fresh else ready[0][0])
                    if not active:
                        if launch_at is not None:
                            queue.progress()
                            time.sleep(min(0.25, max(0, launch_at - now)))
                        continue
                    delay = min(0.25, max(0, launch_at - now)) if launch_at is not None else 0.25
                    completed, _ = wait(active, timeout=delay, return_when=FIRST_COMPLETED)
                    # Observe failures before successes completed in the same wait, so
                    # a concurrent success cannot erase a newly required backoff.
                    for future in sorted(completed, key=lambda item: item.exception() is None):
                        index, prompt, previous, submitted_at = active.pop(future)
                        try:
                            responses[index] = future.result()
                            record = latest.get("prompt:" + prompt.prompt_id)
                            queue.completed(reused=record.get('execution') == 'cross_run_reuse', warmup=not owned_queue)
                            # Saved/reused answers and calls sent before backoff cannot
                            # establish recovery. Only a new successful request can.
                            if (submitted_at >= backoff_until and record is not previous
                                    and record.get("execution") != "cross_run_reuse"):
                                backoff_seconds = 0.0
                            # This prompt is complete and cannot be retried in
                            # this batch. Its validated response is sufficient
                            # for output; the immutable ledger keeps its receipt.
                            latest.pop("prompt:" + prompt.prompt_id)
                        except ExperimentPaused:
                            paused = fatal = True
                        except NewJobLimitReached as limit:
                            failures.append(str(limit) or "new-attempt allowance exhausted")
                            fatal = True
                            queue.stopped.set()
                        except RunnerError:
                            record = latest.get("prompt:" + prompt.prompt_id)
                            # No new receipt means a preflight/provenance/local invariant
                            # failed, not a provider request eligible for retry.
                            if record is previous or record is None:
                                raise
                            kind = prompt_failure_kind(record)
                            now = time.monotonic()
                            if provider_cooldown_required(record) and not fatal:
                                backoff(record, now, submitted_at)
                            delay = provider_retry_delay(record, keep_retrying=keep_retrying_provider_errors)
                            if delay is not None and not fatal:
                                retry_at = now + delay
                                heapq.heappush(ready, (retry_at, index, prompt))
                            else:
                                failures.append(f"{prompt.prompt_id}: {record.get('error')}")
                                fatal |= kind == "provider_fatal"
                                queue.completed(failed=True, warmup=not owned_queue)
                                if fatal:
                                    queue.stopped.set()
                    queue.progress()
            except BaseException:
                queue.stopped.set()
                raise
    finally:
        if owned_queue:
            queue.close()
    if spend is not None and spend.halted:
        failures.append('per-request bound exceeded')
    if failures:
        raise RunnerError("unresolved prompt cases: " + ", ".join(failures))
    if paused:
        return {"status": "paused"}

    output: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "responses": [responses[i] for i in sorted(responses)],
    }
    _atomic_write_json(config.run_directory / "output.json", output, sort_keys=True)
    return output


def _load_mask_batch(
    config_path: Path,
) -> tuple[PromptRunnerConfig, dict[str, Any], dict[str, Any]]:
    path = config_path.expanduser().resolve()
    value = _expect_object(_load_json(path, "config"), "config")
    _expect_fields(value, "config", {
        "schema_version", "runner_binary", "auth_file", "input_path",
        "prompt_path", "run_directory", "model", "reasoning_effort",
    } | ((RUNTIME_FIELDS | {"concurrency", "note_ids", "mask_format", "output_schema"}) & value.keys()))
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise RunnerError("unsupported mask batch schema version")
    concurrency = value.get("concurrency", 1)
    if type(concurrency) is not int or not 1 <= concurrency <= 12:
        raise RunnerError("concurrency must be an integer from 1 to 12")
    source_path = _resolve_path(value["input_path"], "input_path", path.parent)
    template_path = _resolve_path(value["prompt_path"], "prompt_path", path.parent)
    try:
        template = template_path.read_bytes().decode("utf-8")
    except (OSError, UnicodeError) as error:
        raise RunnerError(f"could not read mask prompt: {error}") from error
    if any(template.count(p) != 1 for p in ("{POST}", "{SUBJECT}")):
        raise RunnerError("mask prompt must contain {POST} and {SUBJECT} exactly once")
    mask_format = value.get("mask_format", "fragments")
    if mask_format not in ("fragments", "word_positions"):
        raise RunnerError("mask_format must be fragments or word_positions")
    if mask_format == "word_positions" and template.count("{INDEXED_POST}") != 1:
        raise RunnerError("word_positions requires {INDEXED_POST} exactly once")
    output_schema = value.get("output_schema")
    if output_schema is not None and (not isinstance(output_schema, dict) or output_schema.get("type") != "object"):
        raise RunnerError("output_schema must describe an object")
    source = _expect_object(_load_json(source_path, "input_path"), "input")
    items, prompts = prepare_mask_inputs(source, template, mask_format, value.get("note_ids"))
    config = PromptRunnerConfig(
        config_path=path,
        runner_binary=_resolve_executable(value["runner_binary"], "runner_binary", path.parent),
        **_runtime_options(value, path.parent),
        run_directory=_resolve_path(value["run_directory"], "run_directory", path.parent),
        model=_expect_identifier(value["model"], "model"),
        reasoning_effort=_expect_identifier(value["reasoning_effort"], "reasoning_effort"),
        prompts=tuple(prompts),
        output_schema=output_schema,
    )
    metadata = {"input_path": str(source_path), "input_sha256": _sha256_file(source_path),
                "prompt_path": str(template_path), "prompt_sha256": _sha256_file(template_path),
                "concurrency": concurrency, "items": items}
    if mask_format != "fragments":
        metadata["mask_format"] = mask_format
        metadata["position_decoder_sha256"] = _sha256_file(Path(__file__).with_name("mask_positions.py"))
    return config, metadata, source


def prepare_mask_inputs(source, template, mask_format="word_positions", note_ids=None):
    """Expand saved subjects without leaking their diagnostic context into inputs."""
    if any(template.count(p) != 1 for p in ("{POST}", "{SUBJECT}")):
        raise RunnerError("mask prompt must contain {POST} and {SUBJECT} exactly once")
    if mask_format not in ("word_positions", "fragments"):
        raise RunnerError("mask_format must be fragments or word_positions")
    if mask_format == "word_positions" and template.count("{INDEXED_POST}") != 1:
        raise RunnerError("word_positions requires {INDEXED_POST} exactly once")
    notes = source.get("notes")
    if (type(source.get("schema_version")) is not int or source["schema_version"] != 1
            or not isinstance(notes, list) or not notes):
        raise RunnerError("input must contain schema_version 1 and nonempty notes")
    selected = None
    if note_ids is not None:
        ids = _validate_string_array(note_ids, "note_ids", allow_empty=False)
        selected = set(ids)
        if len(selected) != len(ids):
            raise RunnerError("duplicate note_ids")
    seen = set()
    items, prompts = [], []
    for index, raw in enumerate(notes):
        note = _expect_object(raw, f"notes[{index}]")
        note_id = _expect_string(note.get("note_id"), "note_id")
        if note_id in seen:
            raise RunnerError(f"duplicate note_id `{note_id}`")
        seen.add(note_id)
        if selected is not None and note_id not in selected:
            continue
        text = _expect_string(note.get("text"), "note text")
        subjects = note.get("subjects")
        if not isinstance(subjects, list):
            raise RunnerError("each selected note must have a subjects array")
        for subject_index, raw_subject in enumerate(subjects):
            subject = raw_subject
            if isinstance(subject, dict):
                if "subject" in subject and "text" in subject:
                    raise RunnerError("subject entry must use subject or text, not both")
                subject = subject.get("subject", subject.get("text"))
            subject = _expect_string(subject, "subject")
            if not subject.strip():
                raise RunnerError("subject must not be blank")
            item_id = f"M{len(items) + 1:03}"
            items.append({"item_id": item_id, "note_id": note_id,
                          "note": text, "subject": subject,
                          "subject_index": subject_index})
            # Substitute in the template only, never in text inserted from a note.
            values = {"POST": text, "SUBJECT": subject}
            if mask_format == "word_positions":
                values["INDEXED_POST"] = _position_decoder().index_post(text)
            prompt = re.sub(r"\{(" + "|".join(values) + r")\}", lambda m: values[m[1]], template)
            prompts.append(PromptJob(item_id, prompt))
    if selected is not None and selected - seen:
        raise RunnerError(f"unknown note_ids: {sorted(selected - seen)}")
    if not items:
        raise RunnerError("selected notes have no subjects")
    return items, prompts


@lru_cache(maxsize=1)
def _position_decoder():
    # Also works when extraction_pipeline is loaded by file path outside a package.
    path = Path(__file__).with_name("mask_positions.py")
    spec = importlib.util.spec_from_file_location("mask_positions", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _map_mask_fragments(raw: str, note: str, mask_format: str = "fragments") -> tuple[list[str], list[dict[str, Any]], bool]:
    if mask_format == "word_positions":
        _, ranges = _position_decoder().decode(raw, note)
        fragments, mapped = [], []
        for span in ranges:
            fragment = note[span["start"]:span["end"]]
            if URL_SCHEME_PATTERN.search(fragment) or NIP19_IDENTIFIER.search(fragment):
                raise RunnerError("mask contains a prohibited URL or Nostr identifier")
            positions = [m.start() for m in re.finditer(re.escape(fragment), note)]
            if span["start"] not in positions:
                raise RunnerError("selected range is not representable in the review format")
            fragments.append(fragment)
            mapped.append({"text": fragment, "occurrence": positions.index(span["start"]) + 1})
        return fragments, mapped, False
    fragments = _validate_string_array(json.loads(raw), "mask", allow_empty=False)
    mapped, choices, cursor = [], [], 0
    for fragment in fragments:
        if URL_SCHEME_PATTERN.search(fragment) or NIP19_IDENTIFIER.search(fragment):
            raise RunnerError("mask contains a prohibited URL or Nostr identifier")
        # Match non-overlapping occurrences, like the Rust review tool.
        positions = [m.start() for m in re.finditer(re.escape(fragment), note)]
        available = [(i + 1, pos) for i, pos in enumerate(positions) if pos >= cursor]
        if not available:
            raise RunnerError(f"fragment is absent or out of source order: {fragment!r}")
        occurrence, position = available[0]
        mapped.append({"text": fragment, "occurrence": occurrence})
        choices.append(positions)
        cursor = position + len(fragment)
    # Count valid ordered placements, capped at two: only ambiguity matters.
    counts = {0: 1}
    for fragment, positions in zip(fragments, choices):
        counts = {pos + len(fragment): min(2, sum(n for end, n in counts.items() if end <= pos))
                  for pos in positions}
    return fragments, mapped, sum(counts.values()) > 1


def _collect_mask_review(
    config: PromptRunnerConfig, manifest: dict[str, Any],
    prepared: PreparedManifest, items: list[dict[str, Any]],
) -> dict[str, Any]:
    latest = _load_latest_results(config.run_directory / "results.jsonl",
                                  prepared.manifest_sha256, prepared.provenance)
    review, checks = [], []
    for item, prompt in zip(items, config.prompts):
        check = {"item_id": item["item_id"], "status": "invalid"}
        record = latest.get("prompt:" + prompt.prompt_id)
        try:
            if record is None:
                raise RunnerError("not run")
            if record["status"] == "failed":
                raise RunnerError(f"model call failed: {record.get('error')}")
            validate_prompt_receipt(replace(config, prompts=(prompt,)), manifest, record)
            mask_format = manifest.get("mask_format", "fragments")
            fragments, mapped, ambiguous = _map_mask_fragments(record["final_output_raw"], item["note"], mask_format)
            check.update(status="valid", ambiguous_occurrences=ambiguous,
                         fragments=mapped, raw_fragments=fragments)
            if mask_format == "word_positions":
                diagnostics, ranges = _position_decoder().decode(record["final_output_raw"], item["note"])
                check.update(ranges=ranges, diagnostics={k: v for k, v in diagnostics.items() if k != "mask"})
                if fragments:
                    review.append({**item, "fragments": mapped, "details": "Exact generated positions; not semantically approved."})
            else:
                review.append({**item, "fragments": mapped, "details":
                "Generated mask; not semantically approved. Occurrences use the first placement "
                "compatible with source order; the model returned no positions."})
        except (RunnerError, ValueError) as error:
            check["error"] = str(error)
        checks.append(check)
    ledger = config.run_directory / "results.jsonl"
    records = [json.loads(line) for line in ledger.read_text().splitlines()] if ledger.exists() else []
    usage = {"input_tokens": 0, "cached_input_tokens": 0, "cache_write_tokens": 0,
             "output_tokens": 0, "reasoning_tokens": 0, "attempts_without_usage": 0}
    for record in records:
        receipt = record.get("usage")
        if not isinstance(receipt, dict):
            usage["attempts_without_usage"] += 1
            continue
        for field, sdk_name, fallback in (
            ("input_tokens", "input", "input_tokens"),
            ("cached_input_tokens", "cacheRead", "cached_input_tokens"),
            ("cache_write_tokens", "cacheWrite", "cache_write_tokens"),
            ("output_tokens", "output", "output_tokens"),
            ("reasoning_tokens", "reasoning", "reasoning_output_tokens"),
        ):
            usage[field] += receipt.get(sdk_name, receipt.get(fallback, 0)) or 0
    valid = sum(c["status"] == "valid" for c in checks)
    summary = {"schema_version": 1, "items": len(items), "valid": valid,
               "empty": sum(c["status"] == "valid" and not c["raw_fragments"] for c in checks),
               "invalid": len(items) - valid,
               "saved_attempts": sum(r.get("execution") != "cross_run_reuse" for r in records),
               "reused_results": sum(r.get("execution") == "cross_run_reuse" for r in records),
               "usage": usage,
               "ambiguous_occurrences": sum(c.get("ambiguous_occurrences", False) for c in checks),
               "checks": checks}
    _atomic_write(config.run_directory / "validation.json", _canonical_json_bytes(summary))
    review_path = config.run_directory / "review.json"
    if review:
        _atomic_write(review_path, _canonical_json_bytes({"schema_version": 1,
            "batch_id": "masks-" + prepared.manifest_sha256[:16], "items": review}))
    else:
        review_path.unlink(missing_ok=True)
    return {k: v for k, v in summary.items() if k not in {"checks", "schema_version"}}


def run_masks(
    config_path: Path, *, retry_failed: bool = False,
    prepare_only: bool = False, collect_only: bool = False,
    repeat: bool = False, history_root: Path | None = None,
) -> dict[str, Any]:
    config, metadata, source = _load_mask_batch(config_path)
    if collect_only and not (config.run_directory / "manifest.json").is_file():
        raise RunnerError("no saved mask run to collect")
    config.run_directory.parent.mkdir(parents=True, exist_ok=True)
    # Prevent two invocations from spending on the same unfinished items.
    lock_path = config.run_directory.with_name("." + config.run_directory.name + ".lock")
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RunnerError("mask run is already active") from error
        manifest = {**_build_prompt_manifest(config, _runner_version(config)),
                    "mode": "mask_generation", **metadata}
        _request_history_options(config, manifest, repeat=repeat, history_root=history_root)
        prepared = _prepare_manifest_directory(config.run_directory, manifest)
        _atomic_write(config.run_directory / "input.json", _canonical_json_bytes(source))
        if prepare_only:
            return {"prepared": len(config.prompts), "model_calls": 0}
        error = None
        ledger = config.run_directory / "results.jsonl"
        previous_attempts = sum(json.loads(line).get("execution") != "cross_run_reuse"
                                for line in ledger.read_text().splitlines()) if ledger.exists() else 0
        if not collect_only:
            _validate_auth_file(config.auth_file)
            try:
                _execute_prompt_batch(config, manifest, retry_failed=retry_failed,
                                      concurrency=metadata["concurrency"])
            except RunnerError as caught:
                error = caught
        summary = _collect_mask_review(config, manifest, prepared, metadata["items"])
        summary["new_attempts"] = summary["saved_attempts"] - previous_attempts
        if error is not None:
            raise RunnerError(f"{error}; partial results saved in validation.json") from error
        return summary


def run_direct_prompt(
    prompt: str,
    *,
    runner_binary: str,
    auth_file: Path | None,
    provider: str,
    model: str | None,
    reasoning_effort: str | None,
    output_schema: dict[str, object] | None = None,
    max_output_tokens: int | None = None,
    timeout_seconds: int = 600,
) -> str:
    prompt = _expect_string(prompt, "--text")
    resolved_binary = _resolve_executable(
        runner_binary,
        "--runner-binary",
        Path.cwd(),
    )
    resolved_auth = None if auth_file is None else auth_file.expanduser().resolve()
    model = _expect_identifier(model, "--model")
    reasoning_effort = _expect_identifier(
        reasoning_effort,
        "--reasoning-effort",
    )

    with tempfile.TemporaryDirectory(prefix="model-direct-") as temporary:
        temporary_root = Path(temporary).resolve()
        config_path = temporary_root / "config.json"
        run_directory = temporary_root / "run"
        config_value: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "runner_binary": str(resolved_binary),
            "auth_file": None if resolved_auth is None else str(resolved_auth),
            "provider": provider,
            "timeout_seconds": timeout_seconds,
            "run_directory": str(run_directory),
            "model": model,
            "reasoning_effort": reasoning_effort,
            "prompts": [{"prompt_id": "direct", "text": prompt}],
        }
        if output_schema is not None:
            config_value["output_schema"] = output_schema
        if max_output_tokens is not None:
            config_value["max_output_tokens"] = max_output_tokens
        _atomic_write(
            config_path,
            _ordered_json_bytes(config_value)
            if output_schema is not None
            else _canonical_json_bytes(config_value),
        )
        try:
            output = run_prompts(config_path, retry_failed=False)
        except RunnerError as error:
            results_path = run_directory / "results.jsonl"
            last_record: object = None
            if results_path.is_file():
                try:
                    lines = results_path.read_text(encoding="utf-8").splitlines()
                    if lines:
                        last_record = json.loads(lines[-1])
                except (OSError, UnicodeError, json.JSONDecodeError):
                    pass
            if isinstance(last_record, dict):
                detail = last_record.get("stderr")
                if isinstance(detail, str) and detail.strip():
                    raise RunnerError(f"{error}: {detail.strip()}") from error
            raise
        responses = output.get("responses")
        if not isinstance(responses, list) or len(responses) != 1:
            raise AssertionError("direct prompt returned an invalid response list")
        response = responses[0]
        if not isinstance(response, dict) or response.get("prompt_id") != "direct":
            raise AssertionError("direct prompt returned an invalid response record")
        text = response.get("response")
        if not isinstance(text, str) or not text.strip():
            raise AssertionError("direct prompt returned an empty response")
        return text


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run extraction workflows and audited batches through the model runner"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    run_parser = subcommands.add_parser("run", help="run or resume a batch")
    run_parser.add_argument("config", type=Path)
    run_parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="explicitly retry the latest failed attempt for each failed job",
    )
    run_parser.add_argument(
        "--max-new-jobs",
        type=int,
        help="pause successfully after spawning at most this many uncached jobs",
    )
    run_parser.add_argument(
        "--retry-job",
        help="explicitly rerun one saved successful job after semantic review",
    )
    run_parser.add_argument(
        "--retry-reason",
        help="required audit reason for --retry-job",
    )
    run_parser.add_argument(
        "--runner-upgrade-reason",
        help="audit reason for resuming an existing run with changed wrapper code",
    )
    subjects_parser = subcommands.add_parser(
        "subjects", help="generate atomic subjects without generating masks"
    )
    subjects_parser.add_argument("config", type=Path)
    subjects_parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="explicitly retry the latest failed attempt for each failed subject job",
    )
    subjects_parser.add_argument(
        "--max-new-jobs",
        type=int,
        help="pause successfully after spawning at most this many uncached jobs",
    )
    prompt_parser = subcommands.add_parser(
        "prompt", help="run one direct prompt (saved experiments use experiment.py)"
    )
    prompt_parser.add_argument(
        "config",
        type=Path,
        nargs="?",
        help=argparse.SUPPRESS,
    )
    prompt_parser.add_argument(
        "--text",
        help="direct prompt text, or - to read it from stdin",
    )
    prompt_parser.add_argument("--model")
    prompt_parser.add_argument("--reasoning-effort")
    prompt_parser.add_argument("--auth-file", type=Path)
    prompt_parser.add_argument("--runner-binary")
    prompt_parser.add_argument("--provider")
    prompt_parser.add_argument("--max-output-tokens", type=int)
    prompt_parser.add_argument("--timeout-seconds", type=int)
    prompt_parser.add_argument(
        "--output-schema",
        type=Path,
        help="JSON output schema file for --text",
    )
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _argument_parser().parse_args(arguments)
    try:
        if parsed.command == "prompt":
            if parsed.text is not None:
                if parsed.config is not None:
                    raise RunnerError("use either a config path or --text, not both")
                output_schema = None
                if parsed.output_schema is not None:
                    output_schema = _expect_object(
                        _load_json(
                            parsed.output_schema.expanduser(), "--output-schema"
                        ),
                        "--output-schema",
                    )
                prompt = sys.stdin.read() if parsed.text == "-" else parsed.text
                response = run_direct_prompt(
                    prompt,
                    runner_binary=parsed.runner_binary or str(DEFAULT_RUNNER),
                    auth_file=parsed.auth_file,
                    provider=parsed.provider,
                    model=parsed.model,
                    reasoning_effort=parsed.reasoning_effort,
                    output_schema=output_schema,
                    max_output_tokens=parsed.max_output_tokens,
                    timeout_seconds=parsed.timeout_seconds
                    if parsed.timeout_seconds is not None
                    else 600,
                )
                sys.stdout.write(response)
                return 0
            raise RunnerError("saved batches moved to experiment.py prepare/run; direct calls require --text")
        if parsed.command == "subjects":
            outcome = run_subjects(
                parsed.config,
                retry_failed=parsed.retry_failed,
                max_new_jobs=parsed.max_new_jobs,
            )
            status: dict[str, object] = {}
            if parsed.max_new_jobs is not None:
                status.update(
                    {
                        "complete": outcome.complete,
                        "new_jobs": outcome.new_jobs,
                        "completed_notes": outcome.completed_notes,
                    }
                )
            if outcome.output is not None:
                notes = outcome.output["notes"]
                if not isinstance(notes, list):
                    raise AssertionError("subject output contains invalid notes")
                status["notes"] = len(notes)
                status["subjects"] = sum(len(note["subjects"]) for note in notes)
            print(json.dumps(status, sort_keys=True))
            return 0
        if (parsed.retry_job is None) != (parsed.retry_reason is None):
            raise RunnerError("--retry-job and --retry-reason must be used together")
        if parsed.retry_job is not None and parsed.retry_failed:
            raise RunnerError("--retry-job cannot be combined with --retry-failed")
        if parsed.retry_reason is not None and not parsed.retry_reason.strip():
            raise RunnerError("--retry-reason must not be empty")
        if (
            parsed.runner_upgrade_reason is not None
            and not parsed.runner_upgrade_reason.strip()
        ):
            raise RunnerError("--runner-upgrade-reason must not be empty")
        outcome = run(
            parsed.config,
            retry_failed=parsed.retry_failed,
            max_new_jobs=parsed.max_new_jobs,
            retry_job=parsed.retry_job,
            retry_reason=parsed.retry_reason,
            runner_upgrade_reason=parsed.runner_upgrade_reason,
        )
        status: dict[str, object] = {}
        if parsed.max_new_jobs is not None:
            status.update(
                {
                    "complete": outcome.complete,
                    "new_jobs": outcome.new_jobs,
                    "completed_notes": outcome.completed_notes,
                    "completed_takeaways": outcome.completed_takeaways,
                }
            )
        if outcome.output is not None:
            notes = outcome.output["notes"]
            if not isinstance(notes, list):
                raise AssertionError("pipeline output contains invalid notes")
            status["notes"] = len(notes)
            status["takeaways"] = sum(len(note["takeaways"]) for note in notes)
        print(json.dumps(status, sort_keys=True))
        return 0
    except RunnerError as error:
        print(f"extraction pipeline failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
