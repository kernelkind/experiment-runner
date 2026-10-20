"""Read selected top-level JSON fields without retaining discarded values."""
import ijson
from ijson.common import JSONError, ObjectBuilder


def project(stream, fields):
    """Return selected fields from one complete JSON object read incrementally.

    ``stream`` should return bytes from ``read(size)``. Field names are literal
    top-level keys, not dotted prefixes. Duplicate keys keep their last value,
    including nested objects, as in ``json.load``. Missing keys remain absent.

    Memory follows selected values, the largest parsed token and the parser's
    buffer rather than the complete input. JSON syntax in discarded values and
    trailing content is still checked. Numbers use native ints/floats;
    overflowing numbers rejected by the ijson backend raise ValueError.
    """
    wanted = frozenset(fields)
    events = iter(ijson.basic_parse(stream, use_float=True))
    result = {}
    try:
        if next(events) != ('start_map', None):
            raise ValueError('JSON projection requires a top-level object')
        for event, key in events:
            if event == 'end_map':
                if next(events, None) is not None:
                    raise ValueError('JSON projection rejects trailing content')
                return result
            if event != 'map_key':
                raise ValueError('JSON projection expected an object field')
            builder = ObjectBuilder() if key in wanted else None
            event, value = next(events)
            depth = 0
            while True:
                if builder is not None:
                    builder.event(event, value)
                if event in ('start_map', 'start_array'):
                    depth += 1
                elif event in ('end_map', 'end_array'):
                    depth -= 1
                if not depth:
                    break
                event, value = next(events)
            if builder is not None:
                result[key] = builder.value
    except (JSONError, StopIteration, UnicodeError, OverflowError) as error:
        raise ValueError('invalid JSON object for projection') from error
    raise ValueError('incomplete JSON object for projection')
