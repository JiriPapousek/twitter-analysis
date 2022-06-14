import sys
import os
import json
import locationtagger
import pandas as pd
import sparknlp

from pyspark.ml.feature import SQLTransformer
from sparknlp.pretrained import PretrainedPipeline
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

WINDOW_SIZE = "1 minute"
UPDATE_PERIOD = "30 seconds"


# because of some serialization error when putting this into pandas_udf() 
# this cannot be a method of the TweetProcessor class
def get_nationality(location: pd.Series) -> pd.Series:
    def get_nationality_for_row(location_text: str) -> str:
        if location_text is not None:
            locations = locationtagger.find_locations(text = location_text)

            # primarily get country from its mention in the text
            countries = locations.countries

            # otherwise, get country from whatever else (city, region)
            if len(countries) == 0:
                countries = locations.other_countries
        else:
            countries = ["Unknown"]

        # sometimes we get more estimated nationalities - in that case we take the first one
        return countries[0] if len(countries) != 0 else "Unknown"
    return location.apply(get_nationality_for_row)


class TweetProcessor:

    def __init__(self, args):
        self.bootstrap_servers = args.bootstrap_servers
        self.tweets_topic = args.topic_tweets
        self.tweet_schema = self.get_tweet_schema(args.schema_path)

        self.spark_session = SparkSession \
                             .builder \
                             .appName("processor") \
                             .config("spark.sql.shuffle.partitions", 4) \
                             .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.3.5,com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.4") \
                             .getOrCreate()

        self.db_url = args.db_url
        self.db_user = args.db_user
        self.db_password = args.db_password

    def get_tweet_schema(self, schema_path):
        schema = None
        with open(os.path.join(os.path.split(os.path.abspath(__file__))[0], schema_path)) as json_schema:
            obj = json.loads(json_schema.read())
            schema = StructType.fromJson(obj)
        return schema

    def load_tweets(self):

        tweets =  self.spark_session \
               .readStream \
               .format("kafka") \
               .option("startingOffsets", "earliest") \
               .option("kafka.bootstrap.servers", self.bootstrap_servers) \
               .option("subscribe", self.tweets_topic) \
               .load() \
               .selectExpr("CAST(value AS STRING)") \
               .select(F.col("value")) \
               .select(F.from_json(F.col("value"), schema=self.tweet_schema).alias("data")) \
               .select("data.*") \
               .withColumn("CreatedAt", F.from_unixtime(F.col("CreatedAt") / 1000).cast("timestamp")) \
               .withColumnRenamed("Text", "TweetText") \
               .withColumn("text", F.col("User.Location"))

        pipeline = PretrainedPipeline.from_disk("/models/translate_mul_en_xx_3.1.0_2.4_1622843259436")
        annotations = pipeline.transform(tweets)
        
        translations = SQLTransformer() \
            .setStatement("SELECT *, CAST(element_at(translation, 1).result AS STRING) AS value FROM __THIS__") \
            .transform(annotations)

        return translations

    def retrieve_recent_hashtag_counts(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", WINDOW_SIZE) \
                .withColumn("hashtag", F.explode("HashtagEntities.Text")) \
                .groupBy("hashtag", F.window(timeColumn="CreatedAt", windowDuration=WINDOW_SIZE, slideDuration=UPDATE_PERIOD)) \
                .agg(F.count("*").alias("num"), F.min("window.start").alias("min_start")) \
                .select(F.col("num"), F.col("window.start"), F.col("window.end"), F.col("min_start"), F.col("hashtag"))

    def retrieve_recent_tweet_count(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", WINDOW_SIZE) \
                .groupBy(F.window(timeColumn="CreatedAt", windowDuration=WINDOW_SIZE, slideDuration=UPDATE_PERIOD)) \
                .agg(F.count("*").alias("num")) \
                .select(F.col("num"))

    def retrieve_tweet_counts_per_minute(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", "1 minute") \
                .groupBy(F.window(timeColumn="CreatedAt", windowDuration="1 minute", slideDuration="1 minute")) \
                .agg(F.count("*").alias("num")) \
                .select(F.col("num"), F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"))

    def retrieve_recent_tweet_counts_by_nationality(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", WINDOW_SIZE) \
                .groupBy("nationality", F.window(timeColumn="CreatedAt", windowDuration=WINDOW_SIZE, slideDuration=UPDATE_PERIOD)) \
                .agg(F.count("*").alias("num"), F.min("window.start").alias("min_start")) \
                .select(F.col("num"), F.col("window.start"), F.col("window.end"), F.col("min_start"), F.col("nationality"))


    def retrieve_recent_ukrainian_hashtag_counts(self, tweets):
        return self.retrieve_recent_hashtag_counts(tweets.filter(F.col("nationality") == "Ukraine"))

    def write_output_data_console(self, data, write_mode, trigger_time):
        return data \
               .writeStream \
               .outputMode(write_mode) \
               .format("console") \
               .option("truncate", "false") \
               .trigger(processingTime=trigger_time) \
               .start()
              
    def process(self):
        tweets = self.load_tweets()
        self.write_output_data_console(tweets, "append", "1 second")

        """
        total_tweet_count = self.retrieve_recent_tweet_count(tweets)
        self.write_output_data_postgresql(total_tweet_count,
                                          "total_tweet_count",
                                          "append",
                                          "overwrite",
                                          UPDATE_PERIOD)

        tweet_counts_per_minute = self.retrieve_tweet_counts_per_minute(tweets)
        self.write_output_data_postgresql(tweet_counts_per_minute,
                                          "tweet_counts_per_minute",
                                          "complete",
                                          "overwrite",
                                          "1 minute")

        hashtag_counts = self.retrieve_recent_hashtag_counts(tweets)
        self.write_output_data_postgresql(hashtag_counts, 
                                          "hashtag_counts", 
                                          "append", 
                                          "overwrite", 
                                          UPDATE_PERIOD)

        nationality_counts = self.retrieve_recent_tweet_counts_by_nationality(tweets)
        self.write_output_data_postgresql(nationality_counts,
                                          "nationality_counts",
                                          "append",
                                          "overwrite",
                                          UPDATE_PERIOD)

        ukrainian_hashtag_counts = self.retrieve_recent_ukrainian_hashtag_counts(tweets)
        self.write_output_data_postgresql(ukrainian_hashtag_counts,
                                          "ukrainian_hashtag_counts",
                                          "append",
                                          "overwrite",
                                          UPDATE_PERIOD)
        """
        self.spark_session.streams.awaitAnyTermination()

    def write_output_data_postgresql(self, data, output_table, write_mode, batch_write_mode, trigger_time):

        def write_batch_to_db(batch_dataframe, batch_id):
            # note use of mode 'append' that preserves existing data in the table (alternative 'overwrite')
            print(f"writing {batch_dataframe.count()} rows to {self.db_url}")
            batch_dataframe \
                .write \
                .format("jdbc") \
                .mode(batch_write_mode) \
                .option("url", self.db_url) \
                .option("driver", "org.postgresql.Driver") \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .option("dbtable", output_table) \
                .option("truncate", False) \
                .save()

        return data \
                .writeStream \
                .foreachBatch(write_batch_to_db) \
                .outputMode(write_mode) \
                .trigger(processingTime=trigger_time) \
                .start()

def main():
    parser = ArgumentParser(description="Spark processor for tweets data read from / written to Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap servers", type=str)
    parser.add_argument("--topic-tweets", default="tweets", help="Source Kafka topic with data about tweets", type=str)
    parser.add_argument("--schema-path", default="tweet_schema.json", help="Path to JSON file containing content schema", type=str)
    parser.add_argument("--db-url", default="jdbc:postgresql://localhost:5432/db", help="PostgreSQL database URL used as an output sink", type=str)
    parser.add_argument("--db-user", default="user", help="Username used for connection to PostgreSQL database", type=str)
    parser.add_argument("--db-password", default="user", help="Password used for connection to PostgreSQL database", type=str)
    args = parser.parse_args()

    processor = TweetProcessor(args)
    processor.process()

if __name__ == "__main__":
    main()

