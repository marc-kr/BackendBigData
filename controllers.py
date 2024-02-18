from flask import request
from app import app, spark_repository


@app.route("/devices/count")
def devices_count():
    return spark_repository.devices_count()


@app.route("/tweets/most_retweeted")
def most_retweeted():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_retweeted(limit)


@app.route("/tweets/most_retweeted/by_day")
def most_retweeted_by_day():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_retweeted_tweets_by_day(limit)


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


@app.route("/tweets/day_with_most_count")
def day_with_most_tweets():
    return spark_repository.day_with_most_tweets()


@app.route("/hashtags/total_count")
def hashtags_total_count():
    return f"{spark_repository.hashtags_total_count()}"


@app.route("/hashtags/most_popular")
def most_popular_hashtags():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_popular_hashtags(limit)


@app.route("/hashtags/most_popular/by_day")
def most_popular_hashtags_by_day():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_popular_hashtag_by_day(limit)


@app.route("/biden/tweets/count")
def biden_tweets_count():
    return f"{spark_repository.biden_tweets_count()}"


@app.route("/trump/tweets/count")
def trump_tweets_count():
    return f"{spark_repository.trump_tweets_count()}"


@app.route("/trump/tweets/all")
def trump_tweets():
    limit = int(request.args.get('limit', default=10))
    return f"{spark_repository.trump_tweets(limit)}"


@app.route("/biden/tweets/all")
def biden_tweets():
    limit = int(request.args.get('limit', default=10))
    return f"{spark_repository.biden_tweets(limit)}"


@app.route("/biden/tweets/frequency/daily")
def biden_tweets_daily_frequency():
    return spark_repository.biden_tweet_daily_frequency()


@app.route("/biden/tweets/frequency/hourly")
def biden_tweets_hourly_frequency():
    return spark_repository.biden_tweet_hourly_frequency()


@app.route("/trump/tweets/frequency/daily")
def trump_tweets_daily_frequency():
    return spark_repository.trump_tweet_daily_frequency()


@app.route("/trump/tweets/frequency/hourly")
def trump_tweets_hourly_frequency():
    return spark_repository.trump_tweet_hourly_frequency()


@app.route("/biden/hashtags/all")
def biden_hashtags():
    return spark_repository.biden_hashtags()


@app.route("/biden/hashtags/count")
def biden_hashtags_count():
    return f"{spark_repository.biden_hashtags_count()}"


@app.route("/trump/hashtags/all")
def trump_hashtags():
    return spark_repository.trump_hashtags()


@app.route("/trump/hashtags/count")
def trump_hashtags_count():
    return f"{spark_repository.trump_hashtags_count()}"


@app.route("/biden/tweets/most_popular")
def biden_most_popular():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.biden_most_popular(limit)


@app.route("/trump/tweets/most_popular")
def trump_most_popular():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.trump_most_popular(limit)


@app.route("/biden/hashtags")
def biden_most_used_hashtags():
    return spark_repository.biden_most_used_hashtags()


@app.route("/trump/hashtags")
def trump_most_used_hashtags():
    return spark_repository.trump_most_used_hashtags()


@app.route("/trump/tweets/day_with_most_count")
def trump_most_tweets_day():
    return spark_repository.trump_most_tweets_day()


@app.route("/biden/tweets/day_with_most_count")
def biden_most_tweets_day():
    return spark_repository.biden_most_tweets_day()


@app.route("/biden/tweets/daily_avg")
def biden_tweets_daily_avg():
    return f"{spark_repository.biden_tweets_daily_avg()}"


@app.route("/trump/tweets/daily_avg")
def trump_tweets_daily_avg():
    return f"{spark_repository.trump_tweets_daily_avg()}"


@app.route("/trump/tweets/mentions")
def trump_most_mentions():
    return spark_repository.trump_most_mentions()


@app.route("/biden/tweets/mentions")
def biden_most_mentions():
    return spark_repository.biden_most_mentions()


@app.route("/sentiment/<candidate>/sentiments_count")
def candidate_sentiments_count(candidate):
    return spark_repository.candidate_sentiments_count(candidate)


@app.route("/sentiment/<candidate>/mean_sentiment_score/by_day")
def mean_sentiment_by_day(candidate):
    return spark_repository.mean_sentiment_by_day(candidate)


@app.route("/sentiment/<candidate>/mean_sentiment_score/by_state")
def mean_sentiment_by_state(candidate):
    return spark_repository.mean_sentiment_by_state(candidate)


@app.route("/sentiment/<candidate>/supporters")
def most_supportive_users(candidate):
    limit = int(request.args.get('limit', default=10))
    return spark_repository.candidate_supporters(candidate, limit)


@app.route("/sentiment/<candidate>/haters")
def haters(candidate):
    limit = int(request.args.get('limit', default=10))
    return spark_repository.candidate_haters(candidate, limit)


@app.route("/sentiment/<candidate>/<sentiment>/count/by_day")
def candidate_sentiment_count_by_day(candidate, sentiment):
    return spark_repository.candidate_sentiment_count_by_day(candidate, sentiment)


@app.route("/sentiment/<candidate>/<sentiment>/count/by_state")
def candidate_sentiment_count_by_state(candidate, sentiment):
    return spark_repository.candidate_sentiment_count_by_state(candidate, sentiment)


@app.route("/users/most_mentioned")
def most_mentioned_users():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_mentioned(limit)


@app.route("/users/most_mentioned/by_day")
def most_mentioned_by_day():
    limit = int(request.args.get('limit', default=10))
    return spark_repository.most_mentioned_by_day(limit)
