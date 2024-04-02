import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, udf, when, collect_list
from pyspark.sql.types import StructType
from pyspark.sql import Window
import json

DAY_START = 1
DAY_END = 31

DATASET_FOLDER = './dataset/sentiment'
SCHEMA_DIR = './dataset/sentiment/schema_sa.json'

SAMPLE_FRACTION = 1.0
SAMPLE_SEED = 42

ELECTORAL_VOTES = {
    'AL': 9,
    'AK': 3,
    'AZ': 11,
    'AR': 6,
    'CA': 55,
    'CO': 9,
    'CT': 7,
    'DE': 3,
    'FL': 29,
    'GA': 16,
    'HI': 4,
    'ID': 4,
    'IL': 20,
    'IN': 11,
    'IA': 6,
    'KS': 6,
    'KY': 8,
    'LA': 8,
    'ME': 4,
    'MD': 10,
    'MA': 11,
    'MI': 16,
    'MN': 10,
    'MS': 6,
    'MO': 10,
    'MT': 3,
    'NE': 5,
    'NV': 6,
    'NH': 4,
    'NJ': 14,
    'NM': 5,
    'NY': 29,
    'NC': 15,
    'ND': 3,
    'OH': 18,
    'OK': 7,
    'OR': 7,
    'PA': 20,
    'RI': 4,
    'SC': 9,
    'SD': 3,
    'TN': 11,
    'TX': 38,
    'UT': 6,
    'VT': 3,
    'VA': 13,
    'WA': 12,
    'WV': 5,
    'WI': 10,
    'WY': 3,
    'DC': 3
}


def determine_stance(biden_count, trump_count):
    if biden_count > trump_count:
        return 'biden'
    elif biden_count < trump_count:
        return 'trump'
    return 'neutral'


def tweet_stance(subject, polarity):
    if polarity == 'positive':
        return subject
    return None


def calculate_electoral_votes(states):
    votes = 0
    for state in states:
        votes = votes + ELECTORAL_VOTES[state]
    return votes


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
    file_names = [f"{DATASET_FOLDER}/dataset_sa_{d}.csv" for d in range(DAY_START, DAY_END + 1)]
    schema = StructType.fromJson(json.loads(open(SCHEMA_DIR).read()))
    sa_data = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("nullValue", "NA") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .option("sep", ",") \
        .csv(file_names, schema=schema)  # .sample(fraction=SAMPLE_FRACTION, seed=SAMPLE_SEED)
    sa_data.printSchema()
    tweet_stance_udf = udf(lambda subject, polarity: tweet_stance(subject, polarity))
    sa_data = sa_data.withColumn('stance', tweet_stance_udf(col('subject'), col('sentiment')))

    sa_data = sa_data.where(col('stance').isNotNull())

    user_state = sa_data.where(col('location').isNotNull()) \
        .select('user_id', 'location') \
        .groupBy('user_id', 'location').count() \
        .withColumn('row', row_number()
                    .over(Window.partitionBy('user_id').orderBy(col('count').desc()))) \
        .filter(col('row') == 1) \
        .select('user_id', 'location')

    user_stances = sa_data \
        .groupBy("user_id").pivot("stance", ['biden', 'trump']).count() \
        .withColumn('trump', when(col('trump').isNull(), 0).otherwise(col('trump'))) \
        .withColumn('biden', when(col('biden').isNull(), 0).otherwise(col('biden')))

    user_stances.show()
    determine_stance_udf = udf(lambda x, y: determine_stance(x, y))
    user_stance = user_stances \
        .withColumn('stance', determine_stance_udf(col('biden'), col('trump'))) \
        .select('user_id', 'stance')
    user_stance.show()
    not_neutral_users = user_stance.where("stance != 'neutral'")
    users_locations_stances = not_neutral_users.join(user_state, 'user_id')
    users_locations_stances.show()
    states_result = users_locations_stances.groupBy('location').pivot('stance', ['biden', 'trump']).count()

    states_result = states_result \
        .withColumn('winner', determine_stance_udf(col('biden'), col('trump'))) \
        .withColumn('total', col('biden') + col('trump'))
    states_result.show()
    calculate_electoral_votes_udf = udf(lambda states: calculate_electoral_votes(states))
    states_result\
        .groupBy('winner').agg(collect_list(col('location')).alias('winning_states'))\
        .withColumn('electoral_votes', calculate_electoral_votes_udf(col('winning_states')))\
        .select(col('winner').alias('candidate'), 'electoral_votes').show()

    # states_result.write.json('states_result.json')
