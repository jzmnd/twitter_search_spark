"""Unit tests for `search.py`"""

import pytest

from src.search import get_all_emoji, match_text, filter_adjacent


def test_get_all_emoji(spark, tweet_data, all_emoji):
    """Should return all emoji in the tweet data."""
    result = get_all_emoji(tweet_data)
    assert result.collect() == all_emoji


@pytest.mark.parametrize("window,results", [(1, "match_results_1"), (2, "match_results_2")])
def test_match_text(spark, tweet_data, match_emoji, window, results, request):
    """Should return the results of the regular expression matching on the tweet data."""
    result = match_text(tweet_data, match_emoji, window)
    assert result.collect() == request.getfixturevalue(results)


@pytest.mark.parametrize("window,results", [(1, "match_results_1"), (2, "match_results_2")])
def test_filter_adjacent(spark, before, after, window, results, request):
    """Should return the emoji before and after the match emoji for different window sizes."""
    test_rdd = spark.sparkContext.parallelize(request.getfixturevalue(results))
    result_before = filter_adjacent(test_rdd, window=window, position=-1)
    result_after = filter_adjacent(test_rdd, window=window, position=1)

    assert result_before.collect() == before
    assert result_after.collect() == after
