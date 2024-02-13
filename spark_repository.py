from pyspark.sql import Window
from pyspark.sql.functions import col, day, hour, explode, avg, row_number, struct, collect_list
from constants import *


class SparkRepository:
    def __init__(self, twitter_data, trump_tweets, biden_tweets, sa_data):
        self._twitter_data = twitter_data
        self._trump_tweets = trump_tweets
        self._biden_tweets = biden_tweets
        self._sa_data = sa_data

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

    def most_popular_hashtag_by_day(self, limit):
        result = self._twitter_data \
            .select(explode(col("hashtags")).alias("hashtag"), day("created_at").alias("day")) \
            .groupby("day", "hashtag").count() \
            .withColumn("row",
                        row_number().over(Window.partitionBy("day").orderBy(col("count").desc()))) \
            .filter(col("row") <= limit) \
            .drop("row") \
            .select(col("day"), col("hashtag"), col("count")) \
            .orderBy(col("day").asc()) \
            .collect()
        return [row.asDict() for row in result]

    def hashtags_total_count(self):
        return self._twitter_data \
            .select(explode(col("hashtags"))).distinct().count()

    def tweet_count_by_state(self):
        result = self._twitter_data.where(col("location").isNotNull()).groupby(col("location")).count().collect()
        return [row.asDict() for row in result]

    def day_with_most_tweets(self):
        return self._twitter_data \
            .groupby(day(col('created_at')).alias('day')) \
            .count().orderBy(col('count').desc()).first().asDict()

    def most_retweeted_tweets_by_day(self, limit):
        result = self._twitter_data \
            .withColumn("row",
                        row_number().over(Window.partitionBy(
                            day("created_at")).orderBy(col("retweet_count").desc()))) \
            .filter(col("row") <= limit).drop("row") \
            .select(
                day("created_at").alias("day"), col("tweet_id"), col("text"),
                col("hashtags"), col("retweet_count"),
                col("favorite_count"), col("user_name")).orderBy(col("day").asc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_tweets(self, limit):
        result = self._trump_tweets.limit(limit).collect()
        return [row.asDict() for row in result]

    def biden_tweets(self, limit):
        result = self._biden_tweets.limit(limit).collect()
        return [row.asDict() for row in result]

    def trump_tweets_count(self):
        return self._trump_tweets.count()

    def biden_tweets_count(self):
        return self._biden_tweets.count()

    def biden_hashtags_count(self):
        return self._biden_tweets.select(explode(col("hashtags"))).distinct().count()

    def trump_hashtags_count(self):
        return self._biden_tweets.select(explode(col("hashtags"))).distinct().count()

    def biden_tweet_daily_frequency(self):
        result = self._biden_tweets.groupBy(day(col("created_at")).alias('day')).count().orderBy(
            col('day').asc()).collect()
        return [row.asDict() for row in result]

    def trump_tweet_daily_frequency(self):
        result = self._trump_tweets.groupBy(day(col("created_at")).alias('day')).count().orderBy(
            col('day').asc()).collect()
        return [row.asDict() for row in result]

    def biden_tweet_hourly_frequency(self):
        result = self._biden_tweets.groupBy(hour(col("created_at")).alias('hour')).count().orderBy(
            col('hour').asc()).collect()
        return [row.asDict() for row in result]

    def trump_tweet_hourly_frequency(self):
        result = self._trump_tweets.groupBy(hour(col("created_at")).alias('hour')).count().orderBy(
            col('hour').asc()).collect()
        return [row.asDict() for row in result]

    def biden_hashtags(self):
        result = self._biden_tweets \
            .select(explode(col("hashtags")).alias("hashtag")) \
            .groupBy("hashtag").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_hashtags(self):
        result = self._trump_tweets \
            .select(explode(col("hashtags")).alias("hashtag")) \
            .groupBy("hashtag").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def biden_mentions(self):
        result = self._biden_tweets \
            .select(explode(col("mentions")).alias("mention")) \
            .groupBy("mention").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_mentions(self):
        result = self._trump_tweets \
            .select(explode(col("mentions")).alias("mention")) \
            .groupBy("mention").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_most_popular(self, limit):
        result = self._trump_tweets \
            .orderBy(col("retweet_count").desc()) \
            .limit(limit) \
            .collect()
        return [row.asDict() for row in result]

    def biden_most_popular(self, limit):
        result = self._biden_tweets \
            .orderBy(col("retweet_count").desc()) \
            .limit(limit) \
            .collect()
        return [row.asDict() for row in result]

    def biden_most_used_hashtags(self):
        result = self._biden_tweets.select(explode(col("hashtags")).alias("hashtag")) \
            .groupby(col("hashtag")).count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_most_used_hashtags(self):
        result = self._trump_tweets.select(explode(col("hashtags")).alias("hashtag")) \
            .groupby(col("hashtag")).count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def trump_most_tweets_day(self):
        return self._trump_tweets \
            .groupby(day(col('created_at')).alias('day')) \
            .count().orderBy(col('count').desc()) \
            .first() \
            .asDict()

    def biden_most_tweets_day(self):
        return self._biden_tweets \
            .groupby(day(col('created_at')).alias('day')) \
            .count().orderBy(col('count').desc()) \
            .first() \
            .asDict()

    def biden_tweets_daily_avg(self):
        return self._biden_tweets \
            .groupby(day(col("created_at"))).count() \
            .select(avg(col("count"))) \
            .collect()[0][0]

    def trump_tweets_daily_avg(self):
        return self._trump_tweets \
            .groupby(day(col("created_at"))).count() \
            .select(avg(col("count"))) \
            .collect()[0][0]

    def trump_most_mentions(self):
        result = self._trump_tweets \
            .select(explode(col("mentions")).alias("mention")) \
            .groupBy("mention").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]

    def biden_most_mentions(self):
        result = self._biden_tweets \
            .select(explode(col("mentions")).alias("mention")) \
            .groupBy("mention").count() \
            .orderBy(col("count").desc()) \
            .collect()
        return [row.asDict() for row in result]
