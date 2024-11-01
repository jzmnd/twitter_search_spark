"""Unit tests for `search.py`"""

import re

import pytest

from src.search import get_re, get_all_emoji, match_text, filter_adjacent


@pytest.mark.parametrize("window,pattern", [(1, "pattern_window_1"), (2, "pattern_window_2")])
def test_get_re(match_emoji, window, pattern, request):
    """Should return the correct regular expression for different window sizes."""
    match_re_1 = get_re(match_emoji, window=window)
    assert match_re_1.pattern == request.getfixturevalue(pattern)


def test_get_all_emoji(spark, tweet_data, all_emoji):
    """Should return all emoji in the tweet data."""
    result = get_all_emoji(tweet_data)
    assert result.collect() == all_emoji


@pytest.mark.parametrize(
    "pattern,results",
    [("pattern_window_1", "match_results_1"), ("pattern_window_2", "match_results_2")],
)
def test_match_text(spark, tweet_data, pattern, results, request):
    """Should return the results of the regular expression matching on the tweet data."""
    match_re = re.compile(request.getfixturevalue(pattern))
    result = match_text(tweet_data, match_re)
    assert result.collect() == request.getfixturevalue(results)


@pytest.mark.parametrize("window,results", [(1, "match_results_1"), (2, "match_results_2")])
def test_filter_adjacent(spark, before, after, window, results, request):
    """Should return the emoji before and after the match emoji for different window sizes."""
    test_rdd = spark.sparkContext.parallelize(request.getfixturevalue(results))
    result_before = filter_adjacent(test_rdd, window=window, position=-1)
    result_after = filter_adjacent(test_rdd, window=window, position=1)

    assert result_before.collect() == before
    assert result_after.collect() == after
