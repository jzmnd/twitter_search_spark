#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse


def parse_args():
    """
    Parse the CLI arguments for the Spark job.
    """
    parser = argparse.ArgumentParser(
        description="Search a Twitter archive (from archive.org). "
        "Uses Spark for distributed searching.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-p", "--data_path", default="data", help="Path to the Twitter archive")
    parser.add_argument("-m", "--emoji_match", help="Emoji character to match")
    parser.add_argument("-w", "--window", type=int, default=1, help="Window size for adjacency")
    parser.add_argument(
        "-t", "--top", type=int, default=15, help="Number of top characters to output"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Print info messages")
    return parser.parse_args()
