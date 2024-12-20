#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Utility functions for Spark Twitter search.
"""

from typing import Any, Dict

import pandas as pd


def save_outputs(result: Dict[str, Any], disp: bool = True) -> None:
    """
    Function to create dataframes from the summary result dictionary and output
    .csv files.
    Input:
        result - summary result dictionary
    """

    # Convert output to dataframes
    df_before = pd.DataFrame(result["before_top"], columns=["Emoji", "CountBefore"])
    df_after = pd.DataFrame(result["after_top"], columns=["Emoji", "CountAfter"])
    df_allemoji = pd.DataFrame(result["all_emoji_top"], columns=["Emoji", "Count"])
    df_lang = pd.DataFrame(result["language_counts"], columns=["Lang", "Count"])

    if disp:
        print(df_before)
        print(df_after)
        print(df_lang)
        print(df_allemoji)

    # Merge before and after dataframes
    df_all = pd.merge(df_before, df_after, on="Emoji", how="outer")

    # Output csv files
    df_all.to_csv("./outputs/data.csv", encoding="utf-8", index=False)
    df_lang.to_csv("./outputs/langdata.csv", encoding="utf-8", index=False)
    df_allemoji.to_csv("./outputs/alldata.csv", encoding="utf-8", index=False)
