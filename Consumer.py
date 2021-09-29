from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import findspark
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import json
from Devices.Bulb import *
from Devices.Camera import *
from Devices.Thermostat import *


class Consumer:
    def __init__(self, keyspace, topic, group_id):
        self.keyspace = keyspace
        self.topic = topic
        self. group_id = group_id
        self.table = topic.split("_")[0] + "_device"

    def write_to_cassandra(self, target_df, batch_id):
        target_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.keyspace) \
            .option("table", self.table) \
            .mode("append") \
            .save()
        target_df.show()


if __name__ == "__main__":
    topic_choice = input("Insert the topic:\n[1] bulb_topic\n[2]camera_topic\n[3]term_topic\n")
    group_choice = input("Insert the group of the Consumer: 'a', 'b' or 'c\n")

    topic = None
    schema = None
    if topic_choice == '1':
        schema = Bulb.get_schema()
        topic = "bulb_topic"
    elif topic_choice == '2':
        schema = Camera.get_schema()
        topic = "camera_topic"
    else: 
        schema = Thermostat.get_schema()
        topic = "term_topic"

    consumer = Consumer("kebula", topic, group_choice)

    # Initialize the Spark Session
    spark = SparkSession.builder.appName("KafkaConnection").master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()

    # Open a stream with the Kafka Broker and read data from the Broker
    # ---------------------------------------------------------------------------------------
    # KAFKA OPTIONS: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html 
    # ---------------------------------------------------------------------------------------
    # .option("kafka.group.id", "group_{}".format(group_choice)) \
    raw_data = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    '''
    RAW_DATA FORMAT FROM KAFKA:
    +----+--------------------+----------+---------+------+--------------------+-------------+
    | key|               value|     topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+----------+---------+------+--------------------+-------------+
    Let's extract the values
    '''

    data = raw_data.selectExpr("CAST(value AS STRING)")  # (type(data)): pyspark.sql.dataframe.DataFrame
    '''
    DATA FORMAT (rows in json format):
    +-----------------+
    |            value|
    +-----------------+
    |   {key: value}  |
    +-----------------+
    '''
    # Organize data
    df = data.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Manipulate data
    #df2 = df.groupBy(df.status).count()


    # Manipulate data and print out on the console.
    # Format describes the output source (it could be the Broker, the console or a DB).
    # add a trigger(processingTime='x seconds') to create mini-batch of x second to be processed

    '''
    # Print to console
    query_df = df.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .format("console") \
        .start()
    '''

    # Save to Cassandra
    query_df = df.writeStream \
        .foreachBatch(consumer.write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir-" + topic) \
        .trigger(processingTime="1 minute") \
        .start()

    #query_df2 = df2.writeStream.outputMode("complete").format("console").start()

    '''
    A streaming query runs in a separate daemon thread.
    Waits for the termination of the query, either by: 1)query.stop() or 2)an exception.
    If timeout is set, it returns whether the query has terminated or not within the timeout (seconds).
    '''
    query_df.awaitTermination(timeout=None)
    #query_df2.awaitTermination(timeout=None)

