from pyspark.sql.functions import col, day, hour, explode, avg
from constants import *


class SparkRepository:
    def __init__(self, twitter_data):
        self._twitter_data = twitter_data
        self._trump_tweets = self._twitter_data.where(f"screen_name='{TRUMP_NAME}'")
        self._biden_tweets = self._twitter_data.where(f"screen_name='{BIDEN_NAME}'")

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
