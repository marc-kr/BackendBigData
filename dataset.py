import json

from pyspark.sql.functions import regexp_extract, from_json, when, udf, col
from pyspark.sql.types import ArrayType

from constants import *

DATASET_FOLDER = './dataset'
SCHEMA_DIR = './dataset/schema.json'
SAMPLE_FRACTION = 0.8
SEED = 42

DAY_START = 1
DAY_END = 31


class Dataset:
    def __init__(self, spark):
        self.twitter_data = get_twitter_data(spark)
        self.twitter_data.count()


def get_twitter_data(spark):
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
        .withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType())))

    twitter_data = twitter_data.cache()
    return twitter_data
