from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, from_json, when, col, day, hour, explode, countDistinct, avg
from pyspark.sql.types import ArrayType

from constants import *

SAMPLE_FRACTION = 0.001
SEED = 42

DATASET_FOLDER = "../pythonProject2"


class SparkRepository:
    def __init__(self):
        self._spark = SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
        self._twitter_data = self._read_twitter_data()
        self._trump_tweets = self._twitter_data.where(f"screen_name='{TRUMP_NAME}'")
        self._biden_tweets = self._twitter_data.where(f"screen_name='{BIDEN_NAME}'")

    def _read_twitter_data(self):
        file_names = [f"{DATASET_FOLDER}/tweet_USA_{d}_october.csv" for d in range(DAY_START, DAY_END + 1)]
        twitter_data = self._spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .option("emptyValue", "") \
            .csv(file_names, schema=SCHEMA).sample(fraction=SAMPLE_FRACTION, seed=SEED) \
            .select("tweet_id", "created_at", "user_id_str", "text", "hashtags", "retweet_count", "favorite_count",
                    "in_reply_to_screen_name", "source", "retweeted", "lang", "location", "screen_name")
        # Pulizia della colonna dal codice HTML
        twitter_data = twitter_data.withColumn("source",
                                               regexp_extract(twitter_data["source"],
                                                              r'<a.*?>(.*?)</a>', 1))

        # Conversione della colonna hashtags da stringhe a liste di stringhe
        twitter_data = twitter_data.withColumn("hashtags", from_json(twitter_data["hashtags"], ArrayType(StringType())))
        # Sembra che la colonna retweeted non sia stata costruita bene. Non ci sono valori true nel dataset.
        # Consideriamo retweeted i tweet che iniziano per RT @.
        twitter_data = twitter_data.withColumn("retweeted",
                                               when(twitter_data["text"].rlike(r'^RT @\w+:'), True).otherwise(False))
        return twitter_data.cache()

    def devices_count(self):
        result = self._twitter_data \
            .where(col("source")
                   .isin(["Twitter for iPhone", "Twitter for iPad", "Twitter for Android", "Twitter Web App"])) \
            .groupby("source") \
            .count().collect()

        return [row.asDict() for row in result]

    def most_retweeted(self, limit):
        result = self._twitter_data \
            .dropDuplicates(["text", "retweet_count"]) \
            .orderBy(col("retweet_count").desc()). \
            limit(limit).collect()

        return [row.asDict() for row in result]

    def total_tweets_count(self):
        return self._twitter_data.count()

    def tweet_frequency_daily(self):
        result = self._twitter_data \
            .groupby(day("created_at").alias("day")) \
            .count().orderBy(col("day").asc()) \
            .collect()
        return [row.asDict() for row in result]

    def tweet_frequency_hourly(self):
        result = self._twitter_data \
            .groupby(hour("created_at").alias("hour")) \
            .count().orderBy(col("hour").asc()) \
            .collect()
        return [row.asDict() for row in result]

    def tweet_lang_count(self):
        result = self._twitter_data \
            .groupby("lang") \
            .count() \
            .collect()
        return [row.asDict() for row in result]

    def tweets_daily_avg(self):
        return self._twitter_data.groupby(day(col("created_at"))).count().select(avg(col("count"))).collect()[0][0]

    def most_popular_hashtags(self, limit):
        result = self._twitter_data \
            .select(explode(col("hashtags")).alias("hashtag")) \
            .groupby("hashtag") \
            .count() \
            .orderBy(col("count").desc()) \
            .limit(limit).collect()
        return [row.asDict() for row in result]

    def hashtags_total_count(self):
        return self._twitter_data \
            .select(explode(col("hashtags"))).distinct().count()
