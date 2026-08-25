#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
In-process registries for the two kinds of background work the dashboard
starts: benchmark evaluations, and snapshot fetches.

Both are ``full`` tier, so this is single-operator state and a dict behind a
lock is the right size for it. It would not survive a second replica, which is
noted in the deployment runbook rather than solved here -- the public host runs
neither kind of job.
"""

from __future__ import annotations

import threading
from typing import Dict

from text2sql_eval_toolkit.ui.models import FetchJobStatus, JobStatus

JOBS: Dict[str, JobStatus] = {}
JOBS_LOCK = threading.Lock()


FETCH_JOBS: Dict[str, FetchJobStatus] = {}
FETCH_JOBS_LOCK = threading.Lock()


def update_job(job: JobStatus) -> None:
    with JOBS_LOCK:
        JOBS[job.job_id] = job
