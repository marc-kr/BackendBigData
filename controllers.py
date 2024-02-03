from flask import request
from app import app, spark_repository


@app.route("/devices/count")
def devices_count():
    return spark_repository.devices_count()


@app.route("/tweets/most_retweeted")
def most_retweeted():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_retweeted(limit)


@app.route("/tweets/count/total")
def total_tweets():
    return f"{spark_repository.total_tweets_count()}"


@app.route("/tweets/count/by_state")
def tweets_count_by_state():
    return spark_repository.tweet_count_by_state()


@app.route("/tweets/frequency/daily")
def tweets_frequency_daily():
    return spark_repository.tweet_frequency_daily()


@app.route("/tweets/frequency/hourly")
def tweets_frequency_hourly():
    return spark_repository.tweet_frequency_hourly()


@app.route("/tweets/by_lang/count")
def tweets_lang_count():
    return spark_repository.tweet_lang_count()


@app.route("/tweets/daily_avg")
def tweets_daily_avg():
    return f"{spark_repository.tweets_daily_avg()}"


@app.route("/hashtags/total_count")
def hashtags_total_count():
    return f"{spark_repository.hashtags_total_count()}"


@app.route("/hashtags/most_popular")
def most_popular_hashtags():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_popular_hashtags(limit)
