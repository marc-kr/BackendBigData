from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, mean
from pyspark.sql.types import StructType, StringType, MapType, DoubleType
import sys
import os
import json
from nrclex import NRCLex

DAY_START = 1
DAY_END = 31

DATASET_FOLDER = './dataset/sentiment'
SCHEMA_DIR = './dataset/sentiment/schema_sa.json'

# 1 per caricare l'intero dataset
SAMPLE_FRACTION = 1
SAMPLE_SEED = 42


def emotion_scores(text):
    emotions = NRCLex(text)
    return emotions.affect_frequencies


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder \
        .config('spark.driver.bindAddress', '127.0.0.1')\
        .getOrCreate()
    file_names = [f"{DATASET_FOLDER}/dataset_sa_{d}.csv" for d in range(DAY_START, DAY_END + 1)]
    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    twitter_data = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv(file_names, schema=schema)
    twitter_data = twitter_data.where((col('subject') == 'biden') & (col('sentiment') == 'positive')).sample(fraction=SAMPLE_FRACTION, seed=SAMPLE_SEED)
    emotion_scores_udf = udf(lambda text: emotion_scores(text), MapType(StringType(), DoubleType()))
    twitter_data = twitter_data.withColumn("emotions", emotion_scores_udf(col("text")))
    twitter_data.show(100,truncate=False )
    emotions_scores = twitter_data\
        .select(explode(col("emotions")))\
        .groupBy("key").agg(mean("value").alias("mean"))\
        .collect()
    result = [row.asDict() for row in emotions_scores]
    emotions_scores_dict = {}
    for emotion_score in result:
        emotions_scores_dict[emotion_score['key']] = emotion_score['mean']

    print(emotions_scores_dict)
