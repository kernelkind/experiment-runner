"""Generic cases need no application repository or application adapters."""
import hashlib
import json
import re
from pathlib import Path
from . import extraction_pipeline as engine

_PLACEHOLDER = re.compile(r'\{([A-Z][A-Z0-9_]*)\}')
CACHE_PREFIX_END = '<!-- prompt-cache-end -->'


def digest(value):
    return engine._sha256_json(value)


def without_cache_marker(template):
    if template.count(CACHE_PREFIX_END) > 1:
        raise ValueError(f'prompt template must contain at most one {CACHE_PREFIX_END}')
    return template.replace(CACHE_PREFIX_END, '')


def render(template, inputs):
    template = without_cache_marker(template)
    if not isinstance(inputs, dict) or not inputs:
        raise ValueError('case inputs must be a nonempty object of template variables')
    for key, value in inputs.items():
        if not re.fullmatch(r'[A-Z][A-Z0-9_]*', key) or not isinstance(value, str):
            raise ValueError('template variables must be UPPER_CASE names with string values')
        if '{' + key + '}' not in template:
            raise ValueError(f'unused input variable: {key}')
    def substitute(match):
        if match[1] not in inputs:
            raise ValueError(f'missing template variable: {match[1]}')
        return inputs[match[1]]
    return _PLACEHOLDER.sub(substitute, template)


def template_prefix(template, inputs=None):
    without_cache_marker(template)  # Validate the template before choosing a boundary.
    if CACHE_PREFIX_END in template:
        prefix = template.split(CACHE_PREFIX_END, 1)[0]
        names = {match[1] for match in _PLACEHOLDER.finditer(prefix)}
        if names:
            if inputs is None or names - inputs.keys():
                raise ValueError('declared cache prefix requires its template variable inputs')
            prefix = render(prefix, {name: inputs[name] for name in names})
        return prefix, CACHE_PREFIX_END
    first = _PLACEHOLDER.search(template)
    if first is None:
        raise ValueError('--prompt-cache requires a template input placeholder such as {INPUT_JSON}')
    return template[:first.start()], first.group()


def prepare_file(dataset, path, template, load_report=None):
    if dataset['adapter'] != 'generic':
        raise ValueError('non-generic input requires a project adapter')
    if set(dataset) - {'adapter', 'path', 'select'}:
        raise ValueError('unsupported generic dataset options')
    path = Path(path).resolve()
    raw = path.read_bytes()
    source = json.loads(raw)
    cases = source['cases']
    if not isinstance(cases, list) or not cases:
        raise ValueError('dataset contains no cases')
    ids = set()
    for case in cases:
        engine._expect_fields(case, 'case', {'id', 'inputs'} | ({'evidence', 'mechanical'} & case.keys()))
        identifier = engine._expect_identifier(case['id'], 'case.id')
        if identifier in ids:
            raise ValueError('duplicate case identity')
        ids.add(identifier)
    if 'select' in dataset:
        selected = engine._validate_string_array(dataset['select'], 'dataset.select', allow_empty=False)
        if len(set(selected)) != len(selected) or set(selected) - ids:
            raise ValueError('unknown or duplicate selection IDs')
        cases = [case for case in cases if case['id'] in selected]
    cases = [{**case, 'prompt': render(template, case['inputs']),
              'input_sha256': digest(case['inputs'])} for case in cases]
    return cases, {str(path): hashlib.sha256(raw).hexdigest()}, None


def validate(dataset, case, raw, schema):
    if dataset['adapter'] != 'generic':
        raise ValueError('non-generic input requires a project adapter')
    result = {'status': 'valid', 'output': raw}
    if schema is not None:
        result['parsed'] = json.loads(raw)
    return result


def prominence_comments(envelope):
    return {}


def is_flagged(case):
    return case['status'] != 'valid' or any(
        review.get('verdict') in ('reject', 'uncertain') for review in case.get('reviews', []))
