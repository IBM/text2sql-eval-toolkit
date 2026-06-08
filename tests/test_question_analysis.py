#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from typing import Set
from text2sql_eval_toolkit.profiling.profiling_tools import analyze_question


questions_with_expected_tags = [
    ("How many singers do we have?", {"question_brief", "question_counting"}),
    (
        "In 2012, who had the least consumption in LAM?",
        {"question_moderate", "question_superlative", "question_temporal"},
    ),
    (
        "What was the average monthly consumption of customers in SME for the year 2013?",
        {
            "question_aggregation_intent",
            "question_moderate",
            "question_temporal",
        },
    ),
    (
        "What is the ratio of customers who pay in EUR against customers who pay in CZK?",
        {
            "question_aggregation_intent",
            "question_verbose",
        },
    ),
    (
        "List the names of students who are not enrolled in any course.",
        {
            "question_listing",
            "question_moderate",
            "question_negation",
        },
    ),
    (
        "Which departments have more than 10 employees?",
        {
            "question_comparison",
            "question_listing",
            "question_brief",
        },
    ),
    (
        "Is there any singer from France who has a song in both 2014 and 2015?",
        {
            "question_existence",
            "question_moderate",
            "question_temporal",
        },
    ),
]


@pytest.mark.parametrize("question,expected_tags", questions_with_expected_tags)
def test_analyze_question_tags(question: str, expected_tags: Set[str]):
    assert expected_tags == set(analyze_question(question))
