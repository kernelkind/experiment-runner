"""Explicit project extensions; the executor has no project-code dependency."""
from contextvars import ContextVar
import importlib
import hashlib
import json
from pathlib import Path
import re
import sys

_root = ContextVar('experiment_project_root', default=None)
_configuration = ContextVar('experiment_project_configuration', default={})


def configure(root=None):
    if root is None:
        root = next((p for p in (Path.cwd(), *Path.cwd().parents)
                     if (p / 'experiment-runner.json').is_file() or (p / '.git').exists()), Path.cwd())
    root = Path(root).resolve() if root is not None else None
    configuration = {}
    if root is not None:
        path = root / 'experiment-runner.json'
        if path.exists():
            configuration = json.loads(path.read_text())
            if (set(configuration) != {'schema_version', 'inputs', 'extension'}
                    or type(configuration['schema_version']) is not int
                    or configuration['schema_version'] != 1
                    or any(not isinstance(configuration[k], str)
                           or not re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', configuration[k])
                           for k in ('inputs', 'extension'))):
                raise ValueError('invalid experiment-runner.json project extension')
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
    _root.set(root)
    _configuration.set(configuration)


def root():
    return _root.get()


def import_module(name):
    if not root() or not _configuration.get():
        raise ValueError('this command requires a configured project extension')
    package = _configuration.get()['inputs'].rpartition('.')[0]
    module = importlib.import_module(package + '.' + name)
    if not Path(module.__file__).resolve().is_relative_to(root()):
        raise ValueError('project module was loaded from a different checkout')
    return module


def extension():
    name = _configuration.get().get('extension')
    if not name:
        return None
    module = importlib.import_module(name)
    if not Path(module.__file__).resolve().is_relative_to(root()):
        raise ValueError('project extension was loaded from a different checkout')
    return module


def code_files(dataset):
    directory = Path(__file__).parent
    files = {p.name: p for p in sorted(directory.glob('*.py'))}
    selected = extension()
    if selected:
        files.update(selected.code_files(dataset))
    return files


def prompt_sources(prompt):
    """Validate project-owned prompt composition and freeze its source hashes."""
    selected = extension()
    callback = getattr(selected, 'prompt_sources', None) if selected else None
    if callback is None:
        return {}
    sources = callback(prompt)
    if not isinstance(sources, dict):
        raise ValueError('project prompt_sources must return a source-hash mapping')
    for name, expected in sources.items():
        if (not isinstance(name, str) or not Path(name).is_absolute()
                or not isinstance(expected, str) or not re.fullmatch('[0-9a-f]{64}', expected)):
            raise ValueError('project prompt_sources needs absolute paths and SHA-256 hashes')
        if hashlib.sha256(Path(name).read_bytes()).hexdigest() != expected:
            raise ValueError('project prompt source changed: ' + name)
    return sources


class InputAdapters:
    def __getattr__(self, name):
        selected = _configuration.get().get('inputs')
        if selected:
            module = importlib.import_module(selected)
            if not Path(module.__file__).resolve().is_relative_to(root()):
                raise ValueError('input adapters were loaded from a different checkout')
        else:
            from . import inputs as module
        return getattr(module, name)


inputs = InputAdapters()
