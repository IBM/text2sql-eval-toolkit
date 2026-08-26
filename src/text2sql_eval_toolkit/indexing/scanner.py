#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Streaming byte-range scanner for the toolkit's evaluation artifacts.

Evaluation files are a JSON array of record objects and can reach hundreds of
megabytes, so the dashboard must not load one to read a single record.  This
module walks the file once with bounded memory and reports the exact byte range
of every top-level object, which lets the index store an offset per record and
later read just that slice.

Correctness rests on one detail: brace counting is only valid outside string
literals, so the scanner tracks string state and backslash escapes, including
when an escape straddles a read boundary.
"""

from __future__ import annotations

import re
from typing import BinaryIO, Iterator, NamedTuple

# Outside a string only quotes and braces matter; inside one, only quotes and
# backslashes.  Scanning for the union lets the loop skip everything else.
_STRUCTURAL = re.compile(rb'[\\"{}]')

DEFAULT_CHUNK_SIZE = 1 << 20  # 1 MiB


class RecordSpan(NamedTuple):
    """Byte range of one top-level object, and its raw bytes."""

    start: int
    end: int  # exclusive
    raw: bytes


def iter_record_spans(
    fh: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Iterator[RecordSpan]:
    """
    Yield one :class:`RecordSpan` per top-level object in a JSON array.

    Memory is bounded by ``chunk_size`` plus the largest single record.  The
    file object must be opened in binary mode and positioned at the start.

    Nested objects are not yielded -- only depth-0 braces open and close a span.
    """
    depth = 0
    in_string = False
    pending_escape = False
    start: int | None = None
    buffer = bytearray()  # accumulates the current record only
    base = 0  # absolute offset of chunk[0]

    while True:
        chunk = fh.read(chunk_size)
        if not chunk:
            break

        # An escape that straddled the boundary consumes this chunk's first byte.
        scan_from = 0
        if pending_escape:
            pending_escape = False
            scan_from = 1

        pos = scan_from
        while True:
            match = _STRUCTURAL.search(chunk, pos)
            if match is None:
                break
            i = match.start()
            byte = chunk[i]

            if in_string:
                if byte == 0x5C:  # backslash
                    if i + 1 >= len(chunk):
                        pending_escape = True
                        pos = len(chunk)
                        break
                    pos = i + 2  # skip the escaped character
                    continue
                if byte == 0x22:  # quote
                    in_string = False
            else:
                if byte == 0x22:
                    in_string = True
                elif byte == 0x7B:  # {
                    if depth == 0:
                        start = base + i
                        buffer.clear()
                    depth += 1
                elif byte == 0x7D:  # }
                    depth -= 1
                    if depth == 0:
                        assert start is not None
                        end = base + i + 1
                        # Tail of this record that lives in the current chunk.
                        tail_from = max(0, start - base)
                        buffer += chunk[tail_from : i + 1]
                        yield RecordSpan(start, end, bytes(buffer))
                        buffer.clear()
                        start = None
                    elif depth < 0:
                        raise ValueError(
                            f"Unbalanced '}}' at byte {base + i}: malformed JSON array"
                        )
            pos = i + 1

        # Carry the in-progress record across the boundary.
        if start is not None:
            buffer += chunk[max(0, start - base) :]

        base += len(chunk)

    if depth != 0 or in_string:
        raise ValueError("Truncated or malformed JSON: unbalanced structure at EOF")
