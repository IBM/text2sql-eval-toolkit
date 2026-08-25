#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Trimming result dataframes before they are sent to a browser.

Records carry the full result set of every query they ran, and some of those are
large: one Beaver record holds an 86,502-row ground truth alongside a 55,817-row
prediction. The detail panel that displays them is a 240-pixel scroll box, so
sending all of it produced 854,563 DOM nodes and 858 MB of JS heap to show about
eight visible rows.

So a response carries a head of the rows plus the true count, and the reader is
told what they are looking at. The count matters as much as the trim: a table
silently showing 200 of 86,502 rows is a table that lies about the query's
result.

The stored artifact is untouched -- this is a display concern. The whole record
is still available from the record endpoint for anyone who wants it.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

#: Rows sent per dataframe. Comfortably more than the panel can show, so
#: scrolling still works, and small enough that a hundred of them would not
#: trouble a browser.
MAX_PREVIEW_ROWS = 200


def truncate_dataframe(
    raw: Any, max_rows: int = MAX_PREVIEW_ROWS
) -> Tuple[Any, Optional[int], bool]:
    """
    Return ``(dataframe, total_rows, truncated)``.

    ``dataframe`` keeps the shape it arrived in -- a pandas ``orient='split'``
    JSON string stays a JSON string -- so callers and the UI need no new parsing
    path. Anything this does not recognise is passed through untouched rather
    than dropped: an unfamiliar shape is not a reason to show the reader
    nothing.
    """
    if raw is None:
        return None, None, False

    was_string = isinstance(raw, str)
    if was_string:
        try:
            parsed = json.loads(raw)
        except (ValueError, TypeError):
            return raw, None, False
    else:
        parsed = raw

    if not isinstance(parsed, dict):
        return raw, None, False
    data = parsed.get("data")
    if not isinstance(data, list):
        return raw, None, False

    total = len(data)
    if total <= max_rows:
        return raw, total, False

    trimmed: Dict[str, Any] = dict(parsed)
    trimmed["data"] = data[:max_rows]
    index = parsed.get("index")
    if isinstance(index, list):
        trimmed["index"] = index[:max_rows]

    if was_string:
        return json.dumps(trimmed, ensure_ascii=False), total, True
    return trimmed, total, True
