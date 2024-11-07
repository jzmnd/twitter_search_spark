import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.schema import BASIC_TWEET_SCHEMA


@pytest.fixture(scope="session")
def spark():
    """SparkSession fixture."""
    conf = SparkConf()
    conf.setMaster("local").setAppName("unit-tests")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def match_emoji():
    """Emoji to match on."""
    return "😊"


@pytest.fixture
def tweet_data(spark):
    """Tweet data fixture."""
    return spark.createDataFrame(
        [
            {"id": 1, "text": "has no emoji character", "lang": "en"},
            {"id": 2, "text": "😊", "lang": "en"},
            {"id": 3, "text": "before 😊 after", "lang": "en"},
            {"id": 4, "text": "😊 at start of tweet", "lang": "en"},
            {"id": 5, "text": "tweet ending in 😊", "lang": "en"},
            {"id": 6, "text": "😊 and then two emoji 😊😊", "lang": "en"},
            {"id": 7, "text": "😊 and then two emoji 😍😍", "lang": "en"},
            {"id": 8, "text": "has an emoji 😊😆 after", "lang": "en"},
            {"id": 9, "text": "has an emoji before 😍😊", "lang": "en"},
            {"id": 10, "text": "has an emoji before 😍😊😆 and after", "lang": "en"},
        ],
        BASIC_TWEET_SCHEMA,
    )


@pytest.fixture
def all_emoji():
    """All emoji counts."""
    return [
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😊", 1),
        ("😍", 1),
        ("😍", 1),
        ("😊", 1),
        ("😆", 1),
        ("😍", 1),
        ("😊", 1),
        ("😍", 1),
        ("😊", 1),
        ("😆", 1),
    ]


@pytest.fixture
def match_results_1():
    """Results of regex match for a window size of 1."""
    return [
        ("", "😊", ""),
        ("before", "😊", "after"),
        ("", "😊", "at"),
        ("in", "😊", ""),
        ("", "😊", "and"),
        ("emoji", "😊", "😊"),
        ("😊", "😊", ""),
        ("", "😊", "and"),
        ("emoji", "😊", "😆"),
        ("😍", "😊", ""),
        ("😍", "😊", "😆"),
    ]


@pytest.fixture
def match_results_2():
    """Results of regex match for a window size of 2."""
    return [
        ("", "", "😊", "", ""),
        ("", "before", "😊", "after", ""),
        ("", "", "😊", "at", "start"),
        ("ending", "in", "😊", "", ""),
        ("", "", "😊", "and", "then"),
        ("two", "emoji", "😊", "😊", ""),
        ("emoji", "😊", "😊", "", ""),
        ("", "", "😊", "and", "then"),
        ("an", "emoji", "😊", "😆", "after"),
        ("before", "😍", "😊", "", ""),
        ("before", "😍", "😊", "😆", "and"),
    ]


@pytest.fixture
def before():
    """Adjacent emoji counts before match."""
    return [
        ("😊", 1),
        ("😍", 1),
        ("😍", 1),
    ]


@pytest.fixture
def after():
    """Adjacent emoji counts after match."""
    return [
        ("😊", 1),
        ("😆", 1),
        ("😆", 1),
    ]
