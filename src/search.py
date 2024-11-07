#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Spark search functions for processing Tweet data.
"""
import re
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

from src.unicode_codes import EMOJI_UNICODE_SET

# Matches on a word (including hashtags and mentions) or a non-word character (excluding spaces)
WORD_OR_EMOJI_RE = re.compile(r"['#@]?\w[\w'-]*|[^a-zA-Z0-9_\s]")


def get_all_emoji(df: DataFrame) -> RDD:
    """
    Get all emoji from all tweets and assign a value of 1 to each occurrence for counting.
    Input:
        df - tweet data, should contain a `text` column
    """
    results = df.rdd.flatMap(lambda row: (c for c in row.text if c in EMOJI_UNICODE_SET))
    return results.map(lambda t: (t, 1))


def match_text(df: DataFrame, match: str, window: int) -> RDD:
    """
    Find every occurrence of the match and the neighboring characters or words given a window size.
    Input:
        df     - tweet data, should contain a `text` column
        match  - character to match
        window - find this many emoji or words before and after the match
    """

    def find_all_matches(text: str, match: str, window: int) -> List[Tuple[str, ...]]:
        words = re.findall(WORD_OR_EMOJI_RE, text)
        results = []

        for i, word in enumerate(words):
            if word == match:
                r = []
                for w in range(window)[::-1]:
                    if 0 <= i - (w + 1) < len(words):
                        r.append(words[i - (w + 1)])
                    else:
                        r.append("")
                r.append(word)
                for w in range(window):
                    if 0 <= i + (w + 1) < len(words):
                        r.append(words[i + (w + 1)])
                    else:
                        r.append("")
                results.append(tuple(r))

        return results

    return df.rdd.flatMap(lambda row: find_all_matches(row.text, match, window))


def filter_adjacent(r: RDD, window: int, position: int) -> RDD:
    """
    Filter data to capture emoji with a given position in the match window.
    Assign a value of 1 to each occurrence for counting.
    Input:
        r        - result of regular expression matching
        window   - size of the match window
        position - position within the match window
    """
    idx = position + window
    return r.filter(lambda t: (t[idx] in EMOJI_UNICODE_SET)).map(lambda t: (t[idx], 1))
