from orcest.workflow_contract.v1.canonical import canonical_json_bytes, canonical_json_text


def test_sorts_object_keys() -> None:
    assert canonical_json_text({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_no_insignificant_whitespace() -> None:
    assert canonical_json_text({"a": [1, 2, 3]}) == '{"a":[1,2,3]}'


def test_crlf_and_cr_normalized_to_lf() -> None:
    assert canonical_json_text({"a": "line1\r\nline2\rline3"}) == '{"a":"line1\\nline2\\nline3"}'


def test_unicode_normalized_to_nfc() -> None:
    # "e" + combining acute accent (NFD) must canonicalize identically to the
    # precomposed "é" (NFC).
    nfd = "é"
    nfc = "é"
    assert canonical_json_text({"v": nfd}) == canonical_json_text({"v": nfc})


def test_ensure_ascii_false_keeps_utf8() -> None:
    assert canonical_json_text({"v": "café"}) == '{"v":"café"}'


def test_bytes_output_is_utf8() -> None:
    assert canonical_json_bytes({"v": "café"}) == '{"v":"café"}'.encode("utf-8")


def test_nan_and_infinity_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        canonical_json_text({"v": float("nan")})
    with pytest.raises(ValueError):
        canonical_json_text({"v": float("inf")})


def test_non_string_keys_rejected() -> None:
    import pytest

    with pytest.raises(TypeError):
        canonical_json_text({1: "a"})  # type: ignore[dict-item]


def test_stable_across_repeated_calls() -> None:
    obj = {"z": [3, 2, 1], "a": {"y": None, "x": True}}
    assert canonical_json_text(obj) == canonical_json_text(obj)
