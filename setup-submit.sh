#!/bin/bash

# Assumes Spark is installed via homebrew
export SPARK_HOME="/opt/homebrew/opt/apache-spark/libexec"

# Copy emoji data to root to ship with the Spark job
pip install -U -t dependencies -r requirements.txt
cp dependencies/emoji/unicode_codes/emoji.json ./emoji.json

# Zip python source code
cd src && zip -r ../src.zip . && cd ..

# Modify this to run with your own data or emoji match
spark-submit --master local[*] \
             --py-files src.zip \
             --files emoji.json \
             full_search_spark.py \
             --data_path "small_data/01" \
             --emoji_match water_pistol \
             --window 1 \
             --top 15 \
             --verbose
