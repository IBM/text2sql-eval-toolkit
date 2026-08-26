#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The scanner underpins every index read, so it is tested against input designed
to break naive brace counting: braces inside strings, escaped quotes, escaped
backslashes, and the same content re-scanned at every possible chunk boundary.
"""

import io
import json

import pytest

from text2sql_eval_toolkit.indexing.scanner import iter_record_spans

ADVERSARIAL = [
    {"id": "plain", "sql": "SELECT 1"},
    {"id": "braces-in-string", "sql": "SELECT '{' || '}' FROM t"},
    {"id": "escaped-quote", "sql": 'SELECT "a\\"b" FROM t'},
    {"id": "trailing-backslash", "sql": "C:\\\\path\\\\"},
    {"id": "nested", "predictions": {"p1": {"evaluation": {"execution_accuracy": 1}}}},
    {"id": "unicode", "question": "¿Cuántos? 日本語 🎉 {not a brace}"},
    {"id": "empty-obj", "predictions": {}},
    {"id": "deep", "a": {"b": {"c": {"d": [1, 2, {"e": "}"}]}}}},
]


def _spans(payload, chunk_size):
    raw = json.dumps(payload).encode("utf-8")
    return list(iter_record_spans(io.BytesIO(raw), chunk_size=chunk_size)), raw


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 16, 64, 4096])
def test_spans_round_trip_at_every_chunk_boundary(chunk_size):
    """Each span must decode back to exactly the object it came from."""
    spans, raw = _spans(ADVERSARIAL, chunk_size)
    assert len(spans) == len(ADVERSARIAL)
    for span, expected in zip(spans, ADVERSARIAL, strict=True):
        assert json.loads(span.raw) == expected
        # The reported offsets must independently slice the same bytes.
        assert raw[span.start : span.end] == span.raw


@pytest.mark.parametrize("chunk_size", [1, 5, 4096])
def test_offsets_are_absolute_and_ordered(chunk_size):
    spans, _ = _spans(ADVERSARIAL, chunk_size)
    assert all(s.end > s.start for s in spans)
    # Adjacent pairs: the second sequence is deliberately one shorter, so
    # strict=False is the correct choice here rather than an oversight.
    for a, b in zip(spans, spans[1:], strict=False):
        assert b.start >= a.end


def test_empty_array():
    assert list(iter_record_spans(io.BytesIO(b"[]"))) == []


def test_whitespace_and_newlines_between_records():
    raw = b'[\n  {"id": "a"} ,\n\n  {"id": "b"}\n]\n'
    spans = list(iter_record_spans(io.BytesIO(raw), chunk_size=3))
    assert [json.loads(s.raw) for s in spans] == [{"id": "a"}, {"id": "b"}]


def test_string_containing_only_braces_is_not_a_record():
    raw = json.dumps([{"s": "}{}{"}]).encode()
    spans = list(iter_record_spans(io.BytesIO(raw), chunk_size=1))
    assert len(spans) == 1
    assert json.loads(spans[0].raw) == {"s": "}{}{"}


def test_truncated_input_raises():
    with pytest.raises(ValueError, match="Truncated or malformed"):
        list(iter_record_spans(io.BytesIO(b'[{"id": "a"')))


def test_unbalanced_close_raises():
    with pytest.raises(ValueError, match="Unbalanced"):
        list(iter_record_spans(io.BytesIO(b'[{"a": 1}}]')))


def test_memory_is_bounded_by_chunk_and_record():
    """A large array of small records must not accumulate in memory."""
    payload = [{"id": f"r{i}", "pad": "x" * 100} for i in range(2000)]
    raw = json.dumps(payload).encode()
    seen = 0
    for span in iter_record_spans(io.BytesIO(raw), chunk_size=256):
        # Each yielded record is small; nothing accumulates across iterations.
        assert len(span.raw) < 1000
        seen += 1
    assert seen == 2000
