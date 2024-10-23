#!/bin/bash

export SPARK_HOME="/opt/homebrew/opt/apache-spark/libexec"

pip install -U -t dependencies -r requirements.txt
cp dependencies/emoji/unicode_codes/emoji.json ./emoji.json

cd src
zip -r ../src.zip .
cd ..

# Modify this to run with your own data or emoji match
spark-submit --master local[*] \
             --py-files src.zip \
             --files emoji.json \
             full_search_spark.py \
             --data_path "small_data/01" \
             --emoji_match water_pistol \
             --window 1 \
             --top 15
