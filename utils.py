#!/usr/bin/env python
# encoding: utf-8
"""
Utility functions for Spark Twitter search.
"""

import re

__author__ = 'Jeremy Smith'


def get_re(match, window=2):
    """
    Function to return the regular expression to match a character and its
    neighboring characters as set by the window size.
    Input:
        match  - character to match
        window - find this many characters before and after the match
    """
    word_or_emoji_re = "(['#@]?\w[\w'-]*|\W)?"

    r = "{} " + (window - 1) * "*?{} " \
        + "*?({}) " + (window - 1) * "*{} " + "*{}"
    a = [word_or_emoji_re] * window + [match] + [word_or_emoji_re] * window

    return re.compile(r.format(*a))
