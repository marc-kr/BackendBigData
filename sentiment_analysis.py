import os
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, regexp_replace
from pyspark.sql.types import StructType, ArrayType, StringType, FloatType

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

DAY_START = 1
DAY_END = 31

DATASET_FOLDER = './dataset'
SCHEMA_DIR = './schema.json'

SAMPLE_FRACTION = 1.0
SAMPLE_SEED = 42

sid = SentimentIntensityAnalyzer()

URL_REGEXP = r'https\S+'
HANDLE_REGEXP = r'@\w+'


def sentiment_score(text):
    score = sid.polarity_scores(text)
    return score['compound']


def sentiment(score):
    if score >= 0.05:
        return 'positive'
    elif score <= -0.05:
        return 'negative'
    return 'neutral'


def determine_subject(handles, text: str):
    biden_flag = False
    trump_flag = False
    if handles:
        if 'realDonaldTrump' in handles:
            trump_flag = True
        if 'JoeBiden' in handles:
            biden_flag = True
    if not (biden_flag or trump_flag):
        text = text.lower()
        if 'joe' in text or 'biden' in text or 'joe biden' in text:
            biden_flag = True
        if 'trump' in text or 'donald trump' in text:
            trump_flag = True
    if trump_flag and biden_flag:
        return 'both'
    if biden_flag:
        return 'biden'
    elif trump_flag:
        return 'trump'

    return None


def stringify_array(array):
    if array:
        array_len = len(array)
        if array_len == 0:
            return None
        array_str = "["
        for i in range(array_len - 1):
            array_str = array_str + f"\"{array[i]}\","
        array_str = array_str + f"\"{array[array_len - 1]}\"]"
        return array_str
    return None


def determine_stance(sentiment, subject):
    if sentiment == 'neutral':
        return 'neutral'
    elif subject == 'biden' and sentiment == 'positive':
        return 'biden'
    elif subject == 'biden' and sentiment == 'negative':
        return 'trump'
    elif subject == 'trump' and sentiment == 'positive':
        return 'trump'
    elif subject == 'trump' and sentiment == 'negative':
        return 'biden'


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
    file_names = [f"{DATASET_FOLDER}/dataset_{d}.csv" for d in range(DAY_START, DAY_END + 1)]

    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    for day in range(DAY_START, DAY_END + 1):
        print(f"Creating day {day}")
        twitter_data = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .csv(f"{DATASET_FOLDER}/dataset_{day}.csv", schema=schema).sample(fraction=SAMPLE_FRACTION,
                                                                              seed=SAMPLE_SEED) \
            .withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType()))) \
            .withColumn("mentions", from_json(col("mentions"), ArrayType(StringType())))

        sentiment_score_udf = udf(lambda text: sentiment_score(text), returnType=FloatType())
        determine_subject_udf = udf(lambda handles, hashtags, text: determine_subject(handles, hashtags, text))
        stance_udf = udf(lambda sentiment, subject: determine_stance(sentiment, subject))
        stringify_UDF = udf(lambda array: stringify_array(array), StringType())
        sentiment_udf = udf(lambda score: sentiment(score))

        twitter_data = twitter_data.where("lang='en'").drop('lang')
        twitter_data = twitter_data \
            .withColumn('subject',
                        determine_subject_udf(col('mentions'), col('hashtags'), col('text'))) \
            .filter(col('subject').isNotNull() & (col('subject') != 'both'))

        twitter_data = twitter_data.withColumn('text', regexp_replace(col('text'), URL_REGEXP, '')) \
            .withColumn('text', regexp_replace(col('text'), '@', '')) \
            .withColumn('text', regexp_replace(col('text'), '#', '')) \
            .withColumn('text', regexp_replace(col('text'), r'\n', ' ')) \
            .withColumn('text', regexp_replace(col('text'), r'\s+', ' ')) \
            .filter(col('text').isNotNull() & (col('text') != ' ') & (col('text') != ''))
        # twitter_data.explain(mode='extended')
        twitter_data = twitter_data.withColumn('sentiment_score', sentiment_score_udf(col('text')))
        twitter_data = twitter_data.withColumn('sentiment', sentiment_udf(col('sentiment_score')))
        twitter_data = twitter_data.withColumn('stance', stance_udf(col('sentiment'), col('subject')))
        twitter_data = twitter_data \
            .withColumn('hashtags', stringify_UDF(col('hashtags'))) \
            .withColumn('mentions', stringify_UDF(col('mentions')))

        twitter_data = twitter_data.select(
            'tweet_id',
            'text',
            'user_id',
            'subject',
            'sentiment_score',
            'sentiment',
            'stance',
            'location',
            'created_at'
        )
        twitter_data.show()
        # with open(f"./dataset_sa/dataset_{day}_sa_schema.json", "w") as schema_file:
        #    schema_file.write(twitter_data.schema.json())
        twitter_data.write.option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .option("emptyValue", "NA").csv(f"dataset_{day}_sa.csv")
        print(f"END DAY {day}")
