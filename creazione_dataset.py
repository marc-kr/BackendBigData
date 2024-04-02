from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, when, udf, col, regexp_replace, regexp_extract_all, lit
from pyspark.sql.types import StructField, StringType, BooleanType, LongType, TimestampType, StructType, ArrayType
import logging

from const import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', )
SAMPLE_FRACTION = 0.05
SEED = 42

# Directory in cui si trovano i file del dataset
DATASET_FOLDER = '../dataset'

DAY_START = 1
DAY_END = 31

SCHEMA = StructType([
    StructField("tweet_id", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("user_id_str", StringType(), False),
    StructField("text", StringType(), False),
    StructField("hashtags", StringType(), True),
    StructField("retweet_count", LongType(), False),
    StructField("favorite_count", LongType(), False),
    StructField("in_reply_to_screen_name", StringType(), False),
    StructField("source", StringType(), False),
    StructField("retweeted", BooleanType(), False),
    StructField("lang", StringType(), False),
    StructField("location", StringType(), True),
    StructField("place_name", StringType(), True),
    StructField("place_lat", StringType(), True),
    StructField("place_lon", StringType(), True),
    StructField("screen_name", StringType(), False)
])

RT_FROM_REGEXP = r"RT @([A-Za-z0-9_]+):"
RT_REGEXP = r'RT @\w+:'

#Ritorna la rappresentazione di un array in stringa json. Necessario per il salvataggio in file .csv
def stringify_array(array):
    array_len = len(array)
    if array_len == 0:
        return None
    array_str = "["
    for i in range(array_len-1):
        array_str = array_str + f"\"{array[i]}\","
    array_str = array_str + f"\"{array[array_len-1]}\"]"
    return array_str

#Ritorna il codice dello stato degli USA se in location è contenuta una località valida.
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


if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
    for day in range(DAY_START, DAY_END + 1):
        logging.info(f"creating dataset_{day}")
        twitter_data = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .option("emptyValue", "") \
            .csv(f"{DATASET_FOLDER}/tweet_USA_{day}_october.csv", schema=SCHEMA).sample(fraction=SAMPLE_FRACTION,
                                                                                        seed=SEED)

        twitter_data = twitter_data.select("tweet_id",
                                           "text",
                                           col("user_id_str").alias("user_id"),
                                           col("screen_name").alias("user_name"),
                                           "in_reply_to_screen_name",
                                           "created_at",
                                           "hashtags",
                                           "retweet_count",
                                           "favorite_count",
                                           "source",
                                           "retweeted",
                                           "lang",
                                           "location")

        twitter_data = twitter_data.withColumn("source",
                                               regexp_extract(twitter_data["source"],
                                                              r'<a.*?>(.*?)</a>', 1))

        # Fix della colonna 'retweeted'
        twitter_data = twitter_data.withColumn("retweeted",
                                               when(col("text").rlike(RT_REGEXP), True)
                                               .otherwise(False))
        # Estrazione dell'autore originale dei tweet ritwittati
        twitter_data = twitter_data.withColumn("rt_from", regexp_extract(col("text"), RT_FROM_REGEXP, 1))
        # Eliminazione di RT @nome_utente dai tweet
        twitter_data = twitter_data.withColumn("text", regexp_replace(col("text"), RT_REGEXP, ""))

        stringify_UDF = udf(lambda array: stringify_array(array), StringType())
        # Estrazione degli utenti menzionati nei tweet
        twitter_data = twitter_data\
            .withColumn("mentions", stringify_UDF(regexp_extract_all(col("text"), lit(r'@(\w+)'), 1)))

        map_state_UDF = udf(lambda location: map_state(location), StringType())

        twitter_data = twitter_data.withColumn("location", map_state_UDF("location"))

        twitter_data.write.option("header", "true") \
            .option("multiLine", "true") \
            .option("nullValue", "NA") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("sep", ",") \
            .option("emptyValue", "NA").csv(f"dataset_{day}.csv")
        with open(f"dataset_{day}.csv/{day}_schema.json", "w") as schema_file:
            schema_file.write(twitter_data.schema.json())
        logging.info(f"dataset_{day} created")
