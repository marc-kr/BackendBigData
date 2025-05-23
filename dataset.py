import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType

from constants import *

DATASET_FOLDER = './dataset'
SCHEMA_DIR = './dataset/schema.json'
DATASET_SA_FOLDER = './dataset/sentiment'
SA_SCHEMA_DIR = './dataset/sentiment/schema_sa.json'
SAMPLE_FRACTION = 0.5
SEED = 42

DAY_START = 1
DAY_END = 31


class Dataset:
    def __init__(self, spark: SparkSession):
        self.twitter_data = get_twitter_data(spark)
        self.trump_tweets = get_trump_tweets(spark)
        self.biden_tweets = get_biden_tweets(spark)
        self.sa_data = get_sa_data(spark)
        self.twitter_data.count()
        self.trump_tweets.count()
        self.biden_tweets.count()
        self.sa_data.count()


def get_twitter_data(spark: SparkSession) -> DataFrame:
    file_names = [f"{DATASET_FOLDER}/dataset_{d}.csv" for d in range(DAY_START, DAY_END + 1)]

    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    twitter_data = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv(file_names, schema=schema).sample(fraction=SAMPLE_FRACTION, seed=SEED) \
        .withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType()))) \
        .withColumn("mentions", from_json(col("mentions"), ArrayType(StringType())))

    return twitter_data.cache()


def get_biden_tweets(spark: SparkSession) -> DataFrame:
    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    biden_tweets = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv('dataset/biden/biden.csv', schema=schema) \
        .withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType()))) \
        .withColumn("mentions", from_json(col("mentions"), ArrayType(StringType())))

    return biden_tweets.cache()


def get_trump_tweets(spark: SparkSession) -> DataFrame:
    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    trump_tweets = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv('dataset/trump/trump.csv', schema=schema) \
        .withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType()))) \
        .withColumn("mentions", from_json(col("mentions"), ArrayType(StringType())))

    return trump_tweets.cache()


def get_sa_data(spark: SparkSession) -> DataFrame:
    file_names = [f"{DATASET_SA_FOLDER}/dataset_sa_{d}.csv" for d in range(DAY_START, DAY_END + 1)]
    schema = StructType.fromJson(json.loads(open(SA_SCHEMA_DIR).read()))
    sentiment_data = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv(file_names, schema=schema)  # .sample(fraction=SAMPLE_FRACTION, seed=SEED)
    return sentiment_data.cache()
