from pyspark.sql.functions import regexp_extract, from_json, when, udf
from pyspark.sql.types import ArrayType

from constants import *

DATASET_FOLDER = '../pythonProject2'

SAMPLE_FRACTION = 0.0001
SEED = 42

DAY_START = 1
DAY_END = 31


def map_state(location):
    if location:
        if location in STATE_CODES:
            return location
        if location in STATE_CODE_MAPPING.keys():
            return STATE_CODE_MAPPING[location]
        location_split = location.split(',')
        for word in location_split:
            word_stripped = word.strip()
            if word_stripped in STATE_CODES:
                return word_stripped
            if word_stripped in STATE_CODE_MAPPING.keys():
                return STATE_CODE_MAPPING[word_stripped]
    return None


map_state_UDF = udf(lambda location: map_state(location), StringType())


def get_twitter_data(spark):
    file_names = [f"{DATASET_FOLDER}/tweet_USA_{d}_october.csv" for d in range(DAY_START, DAY_END + 1)]
    twitter_data = spark.read \
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
    twitter_data = twitter_data.withColumn("location", map_state_UDF("location"))
    return twitter_data.cache()
