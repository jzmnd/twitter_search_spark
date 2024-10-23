#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Data literal storing emoji names and unicode codes
"""
from emoji.unicode_codes import EMOJI_DATA


__all__ = ["EMOJI_UNICODE", "UNICODE_EMOJI", "EMOJI_UNICODE_SET"]

UNICODE_EMOJI = {k: v["en"] for k, v in EMOJI_DATA.items()}
EMOJI_UNICODE = {v: k for k, v in UNICODE_EMOJI.items()}
EMOJI_UNICODE_SET = set(EMOJI_DATA.keys())
