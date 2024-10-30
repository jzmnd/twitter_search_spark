#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
Spark schema for Tweet data.
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import LongType, StringType, ArrayType, DoubleType


BASIC_TWEET_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("text", StringType(), True),
        StructField("lang", StringType(), True),
        StructField("retweet_count", LongType(), True),
        StructField("favorite_count", LongType(), True),
        StructField("created_at", StringType(), True),
        StructField(
            "geo",
            StructType(
                [
                    StructField("coordinates", ArrayType(DoubleType(), True), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "delete",
            StructType(
                [
                    StructField(
                        "status",
                        StructType(
                            [
                                StructField("id", LongType(), True),
                                StructField("id_str", StringType(), True),
                                StructField("user_id", LongType(), True),
                                StructField("user_id_str", StringType(), True),
                            ]
                        ),
                        True,
                    )
                ]
            ),
            True,
        ),
    ]
)
