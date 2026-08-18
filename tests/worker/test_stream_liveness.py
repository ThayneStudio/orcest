"""Unit tests for the provider-tolerant stream liveness classifier.

Each test feeds a single stdout line (literal JSON, or garbage) into
``classify_line`` and asserts the resulting ``StreamSignal``. Covers raw
Claude API stream events, Claude Code message-level JSON (including tool
calls and tool errors), the api_retry/rate_limit waiting signals, Grok ACP
session updates, and the catch-all "output" classification for anything
that doesn't match a rule.
"""

import json

from orcest.worker.stream_liveness import StreamSignal, classify_line


def test_content_block_delta_is_progress():
    line = json.dumps({"type": "content_block_delta", "index": 0})
    signal = classify_line(line)
    assert signal.kind == "progress"


def test_content_block_start_is_progress():
    line = json.dumps({"type": "content_block_start", "index": 0})
    assert classify_line(line).kind == "progress"


def test_content_block_stop_is_progress():
    line = json.dumps({"type": "content_block_stop", "index": 0})
    assert classify_line(line).kind == "progress"


def test_message_start_is_progress():
    line = json.dumps({"type": "message_start", "message": {}})
    assert classify_line(line).kind == "progress"


def test_assistant_message_no_tool_is_progress_without_tool_name():
    line = json.dumps(
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}
    )
    signal = classify_line(line)
    assert signal.kind == "progress"
    assert signal.tool_name == ""


def test_assistant_message_single_tool_use_yields_tool_name_and_args():
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {"command": "ls -la"},
                    }
                ]
            },
        }
    )
    signal = classify_line(line)
    assert signal.kind == "progress"
    assert signal.tool_name == "Bash"
    assert signal.tool_args == {"command": "ls -la"}


def test_assistant_message_multi_tool_yields_first_tool_block():
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "thinking..."},
                    {"type": "tool_use", "name": "Read", "input": {"file": "a.py"}},
                    {"type": "tool_use", "name": "Write", "input": {"file": "b.py"}},
                ]
            },
        }
    )
    signal = classify_line(line)
    assert signal.kind == "progress"
    assert signal.tool_name == "Read"
    assert signal.tool_args == {"file": "a.py"}


def test_user_message_tool_result_error_yields_error_class():
    line = json.dumps(
        {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "is_error": True,
                        "content": "command not found: foobar\nsome more detail",
                    }
                ]
            },
        }
    )
    signal = classify_line(line)
    assert signal.kind == "progress"
    assert signal.tool_error_class == "command not found: foobar"


def test_user_message_tool_result_error_class_truncated_to_120_chars():
    long_error = "x" * 300
    line = json.dumps(
        {
            "type": "user",
            "message": {
                "content": [{"type": "tool_result", "is_error": True, "content": long_error}]
            },
        }
    )
    signal = classify_line(line)
    assert signal.kind == "progress"
    assert len(signal.tool_error_class) == 120
    assert signal.tool_error_class == "x" * 120


def test_user_message_tool_result_not_error_is_output():
    line = json.dumps(
        {
            "type": "user",
            "message": {"content": [{"type": "tool_result", "is_error": False, "content": "ok"}]},
        }
    )
    signal = classify_line(line)
    assert signal.kind == "output"
    assert signal.tool_error_class == ""


def test_system_api_retry_is_waiting():
    line = json.dumps({"type": "system", "subtype": "api_retry"})
    signal = classify_line(line)
    assert signal.kind == "waiting"
    assert signal.reason == "api_retry"


def test_rate_limit_event_is_waiting():
    line = json.dumps(
        {
            "type": "rate_limit_event",
            "rate_limit_info": {"status": "blocked", "resetsAt": 12345},
        }
    )
    signal = classify_line(line)
    assert signal.kind == "waiting"
    assert signal.reason == "rate_limit"


def test_grok_acp_method_session_update_is_progress():
    line = json.dumps({"method": "session/update", "params": {}})
    assert classify_line(line).kind == "progress"


def test_grok_acp_session_update_agent_message_chunk_is_progress():
    line = json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sessionUpdate": "agent_message_chunk", "content": {}},
        }
    )
    assert classify_line(line).kind == "progress"


def test_grok_acp_session_update_bare_key_is_progress():
    line = json.dumps({"sessionUpdate": "agent_message_chunk"})
    assert classify_line(line).kind == "progress"


def test_grok_acp_session_update_nested_in_list_is_progress():
    # Spec: the ACP shape may appear "anywhere in the object" -- including
    # inside list values (e.g. a batched "updates" array), not only nested
    # dicts.
    line = json.dumps(
        {
            "jsonrpc": "2.0",
            "result": {
                "updates": [
                    {"other": "noise"},
                    {"sessionUpdate": "agent_message_chunk", "content": {"text": "hi"}},
                ]
            },
        }
    )
    assert classify_line(line).kind == "progress"


def test_malformed_json_is_output():
    signal = classify_line("not json at all {{{")
    assert signal.kind == "output"
    assert signal == StreamSignal(kind="output")


def test_unknown_json_is_output():
    line = json.dumps({"type": "something_unrecognized", "foo": "bar"})
    signal = classify_line(line)
    assert signal.kind == "output"


def test_plain_text_line_is_output():
    signal = classify_line("Reticulating splines...")
    assert signal.kind == "output"


def test_non_dict_json_is_output():
    signal = classify_line(json.dumps([1, 2, 3]))
    assert signal.kind == "output"


def test_empty_line_is_output():
    assert classify_line("").kind == "output"


def test_classify_line_never_raises_on_huge_garbage():
    huge = "{" * 100000
    signal = classify_line(huge)
    assert signal.kind == "output"
