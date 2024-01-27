from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, from_json, when, col
from pyspark.sql.types import ArrayType

from constants import *


class SparkRepository:
    def __init__(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._twitter_data = self._read_twitter_data()

    def _read_twitter_data(self):
        twitter_data = self._spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .option("emptyValue", "") \
            .csv("./tweet_USA_1_october.csv", schema=SCHEMA).limit(1000)

        twitter_data = twitter_data.withColumn("source",
                                               regexp_extract(twitter_data["source"],
                                                              r'<a.*?>(.*?)</a>', 1))

        # Conversione della colonna hashtags da stringhe a liste di stringhe
        twitter_data = twitter_data.withColumn("hashtags", from_json(twitter_data["hashtags"], ArrayType(StringType())))
        # Sembra che la colonna retweeted non sia stata costruita bene. Non ci sono valori true nel dataset. Consideriamo
        # retweeted i tweet che iniziano per RT @.
        twitter_data = twitter_data.withColumn("retweeted",
                                               when(twitter_data["text"].rlike(r'^RT @\w+:'), True).otherwise(False))
        return twitter_data.cache()

    def devices_count(self):
        result = self._twitter_data \
            .where(col("source")
                   .isin(DEVICES.values())) \
            .groupby("source") \
            .count().collect()

        return [row.asDict() for row in result]

    def most_retweeted(self, n):
        result = self._twitter_data \
            .dropDuplicates(["text", "retweet_count"]) \
            .orderBy(col("retweet_count").desc()). \
            limit(n).collect()

        return [row.asDict() for row in result]
