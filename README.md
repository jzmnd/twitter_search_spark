# twitter-search-spark

Script files for searching json Twitter stream. Includes files for finding context of emoji/characters.

Searches a Twitter archive (from archive.org) to find the characters which occur before and after a chosen target.

Uses Spark for parallelization.

Edit `setup-submit.sh` to change the Spark job. Arguments to the `full_search_spark.py` job are:

- `data_path` : Path to the Twitter archive
- `emoji_match` : Name of emoji to match
- `window` : Window size for adjacency
- `top` : Number of top characters to output/display
