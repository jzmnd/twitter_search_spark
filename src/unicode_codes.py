#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Data literal storing emoji names and unicode codes
"""
import json

__all__ = ["EMOJI_UNICODE", "UNICODE_EMOJI", "EMOJI_UNICODE_SET"]


with open("emoji.json", "r") as f:
    # Load from the emoji.json file rather than directly importing from the emoji library since
    # Spark has issues finding this file when inside a zipped python dependency.
    # This also avoids having to ship the whole library to Spark and instead just the data we need.
    EMOJI_DATA = json.load(f)

UNICODE_EMOJI = {k: v["en"] for k, v in EMOJI_DATA.items()}
EMOJI_UNICODE = {v: k for k, v in UNICODE_EMOJI.items()}
EMOJI_UNICODE_SET = set(EMOJI_DATA.keys())
