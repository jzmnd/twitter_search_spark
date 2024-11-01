#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Spark search functions for processing Tweet data.
"""
import re

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

from src.unicode_codes import EMOJI_UNICODE_SET


def get_re(match: str, window: int = 1) -> re.Pattern:
    """
    Function to return the regular expression to match a character and its
    neighboring characters as set by the window size.
    Input:
        match  - character to match
        window - find this many characters before and after the match
    """
    word_or_emoji_re = r"(['#@]?\w[\w'-]*|\W)?"

    r = "{} " + (window - 1) * "*?{} " + "*?({}) " + (window - 1) * "*{} " + "*{}"
    a = [word_or_emoji_re] * window + [match] + [word_or_emoji_re] * window

    return re.compile(r.format(*a))


def get_all_emoji(df: DataFrame) -> RDD:
    """
    Get all emoji from all tweets and assign a value of 1 to each occurrence for counting.
    Input:
        df - tweet data, should contain a `text` column
    """
    results = df.rdd.flatMap(lambda row: (c for c in row.text if c in EMOJI_UNICODE_SET))
    return results.map(lambda t: (t, 1))


def match_text(df: DataFrame, match_re: re.Pattern) -> RDD:
    """
    Perform the regular expression match on text data.
    Input:
        df       - tweet data, should contain a `text` column
        match_re - match regular expression
    """
    return df.rdd.flatMap(lambda row: re.findall(match_re, row.text))


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
