import os
import json

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
                             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1, org.postgresql:postgresql:42.3.5") \
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
        return self.spark_session \
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
               .withColumn("CreatedAt", F.from_unixtime(F.col("CreatedAt") / 1000).cast("timestamp"))

    def retrieve_hashtag_counts(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", "1 minute") \
                .withColumn("hashtag", F.explode("HashtagEntities.Text")) \
                .groupBy("hashtag", F.window(timeColumn="CreatedAt", windowDuration="1 minute", slideDuration="10 seconds")) \
                .agg(F.count("*").alias("num")) \
                .select(F.col("num"), F.col("hashtag"))

    def retrieve_total_tweet_count(self, tweets):
        return tweets \
                .withWatermark("CreatedAt", "1 minute") \
                .groupBy(F.window(timeColumn="CreatedAt", windowDuration="1 minute", slideDuration="10 seconds")) \
                .agg(F.count("*").alias("num")) \
                .select(F.col("num"))

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

        total_tweet_count = self.retrieve_total_tweet_count(tweets)
        self.write_output_data_postgresql(total_tweet_count,
                                          "total_tweet_count",
                                          "append",
                                          "overwrite",
                                          "10 seconds")

        hashtag_counts = self.retrieve_hashtag_counts(tweets)
        self.write_output_data_postgresql(hashtag_counts, 
                                          "hashtag_counts", 
                                          "append", 
                                          "overwrite", 
                                          "10 seconds")

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
    parser.add_argument("--dry-run", help="print results to stdout instead of writing them back to Kafka", action="store_true")
    args = parser.parse_args()

    processor = TweetProcessor(args)
    processor.process()

if __name__ == "__main__":
    main()

