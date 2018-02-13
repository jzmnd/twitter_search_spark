#!/usr/bin/env python
# encoding: utf-8
"""
Search a Twitter archive (from archive.org) to find the characters which
occur before and after a chosen target.
Also counts total number of emoji characters, languages, and locations.

Spark version for data parallelization.

-p  : Path to the Twitter archive
-m  : Emoji to match
-w  : Window size for adjacency
"""

import os
import re
from operator import add
import argparse
from pyspark.sql import SparkSession
from unicode_codes import EMOJI_UNICODE, EMOJI_UNICODE_SET
from timeit import default_timer as timer
from utils import get_re
from utils import output

__author__ = 'Jeremy Smith'


def process(spark, data_dir, match_re, window=1, top=15):
    """
    Main spark process for counting matches and other statistics.
    Input:
        spark - spark session
        data_dir - location of the tweet data files
        match_re - the regular expression for matching
        window - find this many characters before and after the match
        top - number of characters to return in count list
    """
    # Get json files and remove deletes
    files = spark.read.json(os.path.join(data_dir, '*'))
    files_filtered = files.filter(files.delete.isNull())
    cnt = files_filtered.count()

    # Match only to those with required character
    files_match = files_filtered.filter(files_filtered.text.like(match_sql))

    # Select tweet text, language and location info
    tweets = files_match.select(files_match['text'],
                                files_match['lang'],
                                files_match['geo']).rdd.cache()

    # Count tweets
    tweet_cnt = tweets.count()

    # Count languages
    langs_cnt = tweets.map(lambda row: row.lang).countByValue()

    # Perform the regex search
    results = tweets.flatMap(lambda row: re.findall(match_re, row.text)) \
                    .cache()
    before = results.filter(lambda t: (t[0] in EMOJI_UNICODE_SET)) \
                    .map(lambda t: (t[0], 1))
    after = results.filter(lambda t: (t[-1] in EMOJI_UNICODE_SET)) \
                   .map(lambda t: (t[-1], 1))

    # Count matches
    match_cnt = results.count()
    before_cnt = before.count()
    after_cnt = after.count()

    # Counts reduced by emoji character
    before_cnt_by = before.reduceByKey(add)
    after_cnt_by = after.reduceByKey(add)

    # Take ordered list of before and after characters
    before_top = before_cnt_by.takeOrdered(top, key=lambda x: -x[1])
    after_top = after_cnt_by.takeOrdered(top, key=lambda x: -x[1])

    summary_dict = {'before_top': before_top,
                    'after_top': after_top,
                    'match_count': match_cnt,
                    'tweet_count': tweet_cnt,
                    'total_count': cnt,
                    'before_count': before_cnt,
                    'after_count': after_cnt,
                    'language_counts': langs_cnt}

    return summary_dict


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Search a Twitter archive (from archive.org) to find the "
                    "characters which occur before and after a chosen target. "
                    "Uses Spark for distributed searching.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-p', '--data_path',
        default='small_data',
        help="Path to the Twitter archive")
    parser.add_argument(
        '-m', '--emoji_match',
        default='pistol',
        help="Emoji character to match")
    parser.add_argument(
        '-w', '--window',
        type=int,
        default=1,
        help="Window size for adjacency")
    args = parser.parse_args()

    # Match character, SQL search and regex search
    match = EMOJI_UNICODE[':{}:'.format(args.emoji_match)]
    match_sql = '%{}%'.format(match)
    match_re = get_re(match, window=args.window)

    print("Running on data   : {}".format(args.data_path))
    print("Match             : {}  {}".format(args.emoji_match, match))

    # Build SparkSession
    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('full_search_spark') \
                        .config('spark.debug.maxToStringFields', 100) \
                        .getOrCreate()
    sc = spark.sparkContext

    # Main process
    start_t = timer()
    result = process(spark, args.data_path, match_re, args.window)
    end_t = timer()

    print("Elapsed Time          : {:.3f} min".format((end_t - start_t) / 60))
    print("Total Tweets          : {:d}".format(result['total_count']))
    print("Total Tweets w/ Match : {:d}".format(result['tweet_count']))
    print("Total Match Chars     : {:d}".format(result['match_count']))
    print("Total w/ Before       : {:d}".format(result['before_count']))
    print("Total w/ After        : {:d}".format(result['after_count']))

    # Output results
    output(result)

    spark.stop()
