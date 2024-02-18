from flask import Flask
import logging

from pyspark.sql import SparkSession
from dataset import *

from spark_repository import SparkRepository

#logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
spark = SparkSession.builder \
    .config('spark.driver.memory', '3g') \
    .config('spark.executor.memory', '2g') \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()
dataset = Dataset(spark)
spark_repository = SparkRepository(dataset.twitter_data, dataset.trump_tweets, dataset.biden_tweets, dataset.sa_data)

import controllers

if __name__ == '__main__':
    app.run(debug=True)
