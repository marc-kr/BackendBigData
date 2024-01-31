from flask import Flask, request

from spark_repository import SparkRepository

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


@app.route("/tweets/frequency/by_day")
def tweets_frequency_by_day():
    return spark_repository.tweet_frequency_by_day()


@app.route("/tweets/by_lang/count")
def tweets_lang_count():
    return spark_repository.tweet_lang_count()


if __name__ == '__main__':
    app.run(debug=True)
