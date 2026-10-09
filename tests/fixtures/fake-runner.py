#!/usr/bin/env python3
import fcntl
import json
import os
from pathlib import Path
import sys


def append_log(value):
    path = Path(os.environ["FAKE_RUNNER_LOG"])
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        existing = []
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
        existing.append(value)
        path.write_text(json.dumps(existing), encoding="utf-8")


if "--version" in sys.argv:
    print("oracle-model-runner/1 pi-ai/fixture")
    raise SystemExit(0)
if "--identity" in sys.argv:
    print(json.dumps({"runner_version": "oracle-model-runner/1", "runner_sha256": "e" * 64, "pi_ai_version": "fixture"}))
    raise SystemExit(0)

request = json.loads(sys.stdin.read())
prompt = request["prompt"]
argv = sys.argv[1:]
for text, action in json.loads(os.environ.get("FAKE_RUNNER_SEQUENCE", "{}")).items():
    if text in prompt:
        import time
        if action.get("sleep_seconds"):
            time.sleep(action["sleep_seconds"])
        if action.get("touch"):
            Path(action["touch"]).touch()
        until = time.monotonic() + 3
        while action.get("wait_for") and not Path(action["wait_for"]).exists():
            if time.monotonic() > until:
                raise SystemExit("independent case did not start")
            time.sleep(0.01)
if os.environ.get("FAKE_RUNNER_BARRIER"):
    import time
    barrier = Path(os.environ["FAKE_RUNNER_BARRIER"])
    (barrier / str(os.getpid())).touch()
    until = time.monotonic() + 3
    while len(list(barrier.iterdir())) < int(os.environ.get("FAKE_RUNNER_BARRIER_COUNT", "2")):
        if time.monotonic() > until:
            raise SystemExit("expected calls did not run concurrently")
        time.sleep(0.01)
if prompt.startswith("GENERIC PROMPT"):
    stage = "prompt"
elif prompt.startswith("CLAIM INSTRUCTION"):
    stage = "claim"
elif prompt.startswith("SUBJECT INSTRUCTION"):
    stage = "subject"
elif prompt.startswith("MASK INSTRUCTION"):
    stage = "mask"
elif prompt.startswith("PROMINENCE INSTRUCTION"):
    stage = "prominence"
else:
    stage = "takeaway"
append_log({
    "kind": "exec",
    "stage": stage,
    "argv": argv,
    "auth_file": argv[argv.index("--auth-file") + 1] if "--auth-file" in argv else None,
    "request": request,
    "cwd": os.getcwd(),
    "cwd_entries": sorted(os.listdir()),
    "prompt": prompt,
    "output_schema_text": (
        json.dumps(request["schema"], ensure_ascii=False, separators=(",", ":")) + "\n"
        if "schema" in request else None
    ),
    "credential_environment": {
        key: value
        for key, value in os.environ.items()
        if key.startswith(("CODEX_", "OPENAI_", "CHATGPT_"))
    },
})

mutation_path = os.environ.get("FAKE_RUNNER_MUTATE_PATH")
if mutation_path is not None:
    path = Path(mutation_path)
    path.write_bytes(path.read_bytes() + b"\n")

if os.environ.get("FAKE_RUNNER_FAIL_STAGE") == stage:
    print("fixture child failure", file=sys.stderr)
    raise SystemExit(23)

if stage == "prompt":
    response_object = None
    output_text = os.environ.get(
        "FAKE_RUNNER_PROMPT_RESPONSE", f"Response to: {prompt}"
    )
elif stage == "takeaway":
    response = (
        ["Tea helps the author."]
        if os.environ.get("FAKE_RUNNER_REVISED_TAKEAWAYS") == "1"
        else
        []
        if os.environ.get("FAKE_RUNNER_EMPTY_TAKEAWAYS") == "1"
        else [
            "The author finds Rust pleasant.",
            "Tea helps the author.",
        ]
    )
elif stage == "subject":
    response = (
        ["Solar power generation."]
        if "Solar power" in prompt
        else ["Rust programming.", "Tea drinking."]
    )
elif stage == "claim":
    response = (
        ["Solar power will dominate electricity generation."]
        if "Solar power" in prompt
        else ["Rust is pleasant.", "Tea helps."]
    )
elif stage == "prominence":
    invalid_prominence = os.environ.get("FAKE_RUNNER_INVALID_PROMINENCE")
    response = {
        "bool": True,
        "string": "85",
        "low": -1,
        "high": 101,
        "zero": 0,
    }.get(
        invalid_prominence,
        85 if "The author finds Rust pleasant." in prompt else 25,
    )
elif os.environ.get("FAKE_RUNNER_INVALID_MASK") == "1":
    response = ["not present in the source note"]
elif os.environ.get("FAKE_RUNNER_URL_MASK") == "1":
    response = ["https://example.com/source"]
elif "The author finds Rust pleasant." in prompt:
    response = ["Rust is pleasant"]
else:
    response = ["Tea helps"]

if stage != "prompt":
    response_object = {
        {
            "takeaway": "takeaways",
            "claim": "claims",
            "subject": "subjects",
            "prominence": "prominence",
            "mask": "fragments",
        }[stage]: response,
    }
    output_text = json.dumps(response_object)
import hashlib
valid = os.environ.get("FAKE_RUNNER_MISSING_COMPLETION") != "1"
result = {
    "provider": request["provider"], "model": request["model"],
    "reasoning": request["reasoning"], "runner_version": "oracle-model-runner/1",
    "runner_sha256": "e" * 64, "pi_ai_version": "fixture",
    "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
    "schema_sha256": hashlib.sha256(json.dumps(request["schema"], ensure_ascii=False, separators=(",", ":")).encode()).hexdigest() if "schema" in request else None,
    "raw_text": output_text, "output": json.loads(output_text) if "schema" in request else output_text,
    "valid": valid, "error": None, "validation_error": None,
    "stop_reason": "stop" if valid else "length", "http_attempts": 1,
    "usage": {"input_tokens": 100, "cached_input_tokens": 0, "output_tokens": 20, "reasoning_output_tokens": 5},
    "request": {"body": {"model": request["model"], ("input" if request["provider"] == "openai-codex" else "messages"): [{"role": "user", "content": prompt}], **(({"text": {"format": {"schema": request["schema"]}}} if request["provider"] == "openai-codex" else {"response_format": {"json_schema": {"schema": request["schema"]}}}) if "schema" in request else {})}}, "response_metadata": {},
}
if "prompt_cache_key" in request:
    result["request"]["body"]["prompt_cache_key"] = request["prompt_cache_key"]
    result["request"]["headers"] = {"session-id": request["prompt_cache_key"],
                                     "x-client-request-id": request["prompt_cache_key"]}
if "prompt_cache_prefix" in request:
    import uuid
    def content_id(kind, value):
        return kind + "_" + str(uuid.uuid5(uuid.NAMESPACE_DNS, kind + "\0" + json.dumps(value, ensure_ascii=False, separators=(",", ":"))))
    def message(role, text):
        content = [{"type": "input_text", "text": text}]
        return {"type": "message", "id": content_id("msg", {"role": role, "content": content}),
                "role": role, "content": content, "internal_chat_message_metadata_passthrough": {
                    "content_item_kinds": ["model.base_instructions" if role == "developer" else "user.text"]}}
    prefix = request["prompt_cache_prefix"]
    body = result["request"]["body"]
    body["input"] = [{"type": "additional_tools", "id": content_id("at", []), "role": "developer", "tools": []}, message("developer", prefix)]
    if prompt[len(prefix):]: body["input"].append(message("user", prompt[len(prefix):]))
    body.update(tool_choice="auto", parallel_tool_calls=False, include=["reasoning.encrypted_content"],
                reasoning={"effort": request["reasoning"], "context": "all_turns"},
                client_metadata={"session_id": request["prompt_cache_key"], "thread_id": request["prompt_cache_key"]})
    result["request"]["headers"].update({"x-openai-internal-codex-responses-lite": "true",
        "x-codex-routing-hint": "model=gpt-6.1-sol", "thread-id": request["prompt_cache_key"]})
result.update(json.loads(os.environ.get("FAKE_RUNNER_RESULT_OVERRIDE", "{}")))
failures = json.loads(os.environ.get("FAKE_RUNNER_FAILURES", "{}"))
for text, failure in failures.items():
    if text in prompt:
        log = Path(os.environ["FAKE_RUNNER_LOG"])
        with log.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            attempts = sum(c["prompt"] == prompt for c in json.loads(log.read_text()))
        if attempts <= failure.get("attempts", 100):
            result.update(valid=False, error=failure.get("error", "fixture failure"), stop_reason="error",
                          response_metadata=failure["metadata"])
            print(json.dumps(result))
            raise SystemExit(1)
print(json.dumps(result))
