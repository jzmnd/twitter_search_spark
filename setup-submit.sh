#!/bin/bash

export SPARK_HOME="/usr/local/Cellar/apache-spark/2.2.1/libexec"

spark-submit --master local[*] \
             --py-files utils.py \
             full_search_spark.py \
             --data_path "small_data/01" \
             --emoji_match pistol \
             --window 1 \
             --top 15
