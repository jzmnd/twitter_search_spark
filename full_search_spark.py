#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Search a Twitter archive (from archive.org) to find the characters which
occur before and after a chosen target.
Also counts total number of emoji characters, languages, and locations.

Spark version for data parallelization.

-p  : Path to the Twitter archive
-m  : Emoji to match
-w  : Window size for adjacency
-t  : Number of top characters to output
"""

import os
from operator import add
from timeit import default_timer as timer

from pyspark.sql import SparkSession

from src.cli import parse_args
from src.schema import BASIC_TWEET_SCHEMA
from src.search import get_all_emoji, match_text, filter_adjacent
from src.unicode_codes import EMOJI_UNICODE
from src.utils import save_outputs

__author__ = "Jeremy Smith"


def process(spark: SparkSession, data_dir: str, match: str, window: int, top: int):
    """
    Main spark process for counting matches and other statistics.
    Input:
        spark - spark session
        data_dir - location of the tweet data files
        match - the emoji to match
        window - find this many characters before and after the match
        top - number of characters to return in count list
    """
    match_sql = "%{}%".format(match)

    # A reduced json tweet schema for fields of interest
    sc = spark.sparkContext
    schema = sc.broadcast(BASIC_TWEET_SCHEMA)

    # Get json files and remove deletes
    files = spark.read.json(os.path.join(data_dir, "*"), schema.value)
    files_filtered = files.filter(files.delete.isNull())
    cnt = files_filtered.count()

    # Match only to those with required character
    tweets = files_filtered.filter(files_filtered.text.like(match_sql)).cache()

    # Count tweets
    tweet_cnt = tweets.count()

    # Count languages
    langs_cnt = tweets.groupBy("lang").count().collect()

    # Select all emoji from all tweet text
    allemoji = get_all_emoji(files_filtered)

    # Perform the regex search and store before and after emoji
    matches = match_text(tweets, match, window).cache()
    before = filter_adjacent(matches, window, position=-1)
    after = filter_adjacent(matches, window, position=1)

    # Count matches
    match_cnt = matches.count()
    before_cnt = before.count()
    after_cnt = after.count()

    # Take ordered list of before and after character counts
    allemoji_top = allemoji.reduceByKey(add).takeOrdered(top, key=lambda x: -x[1])
    before_top = before.reduceByKey(add).takeOrdered(top, key=lambda x: -x[1])
    after_top = after.reduceByKey(add).takeOrdered(top, key=lambda x: -x[1])

    return {
        "before_top": before_top,
        "after_top": after_top,
        "all_emoji_top": allemoji_top,
        "match_count": match_cnt,
        "tweet_count": tweet_cnt,
        "total_count": cnt,
        "before_count": before_cnt,
        "after_count": after_cnt,
        "language_counts": langs_cnt,
    }


if __name__ == "__main__":
    args = parse_args()

    # Match character
    match = EMOJI_UNICODE[":{}:".format(args.emoji_match)]

    if args.verbose:
        print("Running on data   : {}".format(args.data_path))
        print("Match             : {}  {}".format(args.emoji_match, match))

    # Build SparkSession
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("full_search_spark")
        .config("spark.debug.maxToStringFields", 100)
        .getOrCreate()
    )

    # Main process
    start_t = timer()
    result = process(spark, args.data_path, match, window=args.window, top=args.top)
    end_t = timer()

    spark.stop()

    if args.verbose:
        print("Elapsed Time          : {:.3f} min".format((end_t - start_t) / 60))
        print("Total Tweets          : {:d}".format(result["total_count"]))
        print("Total Tweets w/ Match : {:d}".format(result["tweet_count"]))
        print("Total Match Chars     : {:d}".format(result["match_count"]))
        print("Total w/ Before       : {:d}".format(result["before_count"]))
        print("Total w/ After        : {:d}".format(result["after_count"]))

    # Output results
    save_outputs(result, disp=args.verbose)
