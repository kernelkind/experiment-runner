"""Exact word-position masks, including diagnostic and valid-empty responses.

The position rules are the existing position-selection-v6 decoder's rules.
Offsets returned here are Python character offsets, not UTF-8 byte offsets.
"""
import json
import re


def index_post(source):
    words = iter(range(1, len(re.findall(r"\S+", source)) + 1))
    return re.sub(r"\S+", lambda m: f"[{next(words)}]" + m.group(), source)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse_json_response(raw):
    """Accept bare JSON or one complete Markdown JSON fence, preserving validation."""
    fenced = re.fullmatch(r"\s*```(?:json)?[ \t]*\r?\n(.*?)\r?\n```\s*", raw,
                          flags=re.DOTALL | re.IGNORECASE)
    return json.loads(fenced.group(1) if fenced else raw, object_pairs_hook=unique_object)


def decode(raw, source):
    response = parse_json_response(raw)
    if not isinstance(response, dict) or set(response) != {"meaning", "expressible", "evidence", "mask"}:
        raise ValueError("expected meaning, expressible, evidence and mask fields")
    for key in ("meaning", "evidence"):
        if not isinstance(response[key], str) or not response[key].strip():
            raise ValueError(f"{key} must be a nonempty string")
    if type(response["expressible"]) is not bool or not isinstance(response["mask"], list):
        raise ValueError("expected boolean expressible and array mask")
    words = list(re.finditer(r"\S+", source))
    ranges, previous_end = [], 0
    for item in response["mask"]:
        if isinstance(item, list):
            if len(item) != 2 or any(type(v) is not int for v in item):
                raise ValueError("whole-word range must contain two integer positions")
            first, last = item
            if not 1 <= first <= last <= len(words):
                raise ValueError("invalid inclusive word positions")
            start, end = words[first - 1].start(), words[last - 1].end()
        elif isinstance(item, dict) and item.keys() == {"word", "text"}:
            position, text = item["word"], item["text"]
            if type(position) is not int or not 1 <= position <= len(words):
                raise ValueError("invalid word position")
            if not isinstance(text, str) or re.fullmatch(r"\S+", text) is None:
                raise ValueError("selected text must be nonempty without whitespace")
            word = words[position - 1]
            offset = word.group().find(text)
            if offset < 0 or word.group().find(text, offset + 1) >= 0:
                raise ValueError("selected text must occur exactly once in the addressed word")
            start, end = word.start() + offset, word.start() + offset + len(text)
        else:
            raise ValueError("expected a whole-word range or word/text selection")
        if start < previous_end:
            raise ValueError("overlapping or unordered selection")
        ranges.append({"start": start, "end": end})
        previous_end = end
    if response["expressible"] != bool(ranges):
        raise ValueError("expressible verdict contradicts empty/nonempty mask")
    return response, ranges
