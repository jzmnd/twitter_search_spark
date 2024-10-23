# twitter-search-spark

PySpark script files for searching json Twitter stream data.
Primarily for finding the context of a particular emoji/character.

Searches Twitter archive data (from archive.org) to find the characters which occur before and after a chosen target within a certain window.
This is useful for analyzing how emoji are used in context and how they are combined.
For more information see the original non-Spark version [here](https://github.com/janesolomon/twitter_search).

Uses Spark for parallelized read of large Twitter datasets.

Edit `setup-submit.sh` to change the Spark job.
Arguments to the `full_search_spark.py` job are:

- `data_path` : Path to the Twitter archive
- `emoji_match` : Name of emoji to match
- `window` : Window size for adjacency
- `top` : Number of top characters to output/display
- `verbose` : Print info messages
