from flask import Flask, request
import logging

from spark_repository import SparkRepository

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
spark_repository = SparkRepository()


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.route("/devices/count")
def devices_count():
    return spark_repository.devices_count()


@app.route("/tweets/most_retweeted")
def most_retweeted():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_retweeted(limit)


@app.route("/tweets/total_count")
def total_tweets():
    return f"{spark_repository.total_tweets_count()}"


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


if __name__ == '__main__':
    app.run(debug=True)
