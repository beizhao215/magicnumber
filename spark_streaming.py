from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import pyspark_cassandra, sys
from pyspark.sql import Window
from pyspark import StorageLevel

import json

from pyspark.sql.types import DoubleType, TimestampType, StringType
from datetime import datetime
from pyspark.sql.functions import date_format, sum, avg, udf
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame


def main():
    """ Joins two input streams to get location specidic rates,
        integrates the rates in a sliding window to calculate qum quantity,
        saves the results to Cassandra """

    if len(sys.argv) != 4:
        print("Usage: kafka_wordcount.py <zk> <sensor_topic> <room_topic>", file=sys.stderr)
        exit(-1)


    # Kafka and Spark Streaming specific vars
    batch_length = 5
    #window_length = 50

    sc = SparkContext(appName="magicNumberStreaming")
    ssc = StreamingContext(sc, batch_length)
    ssc.checkpoint("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/usr/stream_checkpoint")

    zkQuorum, topic = sys.argv[1:]
    # Specify all the nodes you are running Kafka on
    kafkaBrokers = {"metadata.broker.list": "54.70.249.2:9092, 52.27.243.234:9092, 54.69.226.73:9092, 54.68.152.133:9092"}

    # Get the sensor and location data streams - they have separate Kafka topics
    trips_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)


    ##### push to Cassandra #####

    #raw_loc = raw_data_tojson(loc_data)


    trips_info = trips_data.persist(StorageLevel.MEMORY_ONLY)

    # Group stream by room and calc average rate signal
    room_rate_gen = combined_info.map(lambda x: ((x[1][0],x[0][1]), x[1][1])).groupByKey().\
                              map(lambda x : (x[0][0], (x[0][1], reduce(lambda x, y: x + y, list(x[1]))/float(len(list(x[1])))))).persist(StorageLevel.MEMORY_ONLY)

    room_rate = room_rate_gen.map(lambda x: {"room_id": x[0], "timestamp": x[1][0], "rate": x[1][1]})
    room_rate.saveToCassandra("rate_data", "room_rate")

    # Find which users are at a certain room
    room_users = combined_info.map(lambda x: ((x[1][0],x[0][1]), x[0][0])).groupByKey().\
                               map(lambda x : {"room_id": x[0][0], "timestamp": x[0][1], "users": list(x[1])})
    room_users.saveToCassandra("rate_data", "room_users")


    # Save all user info (id, time, rate, room) to Cassandra
    user_rate = combined_info.map(lambda x: { "user_id": x[0][0], "timestamp": x[0][1],  "rate": x[1][1], "room": x[1][0]})
    user_rate.saveToCassandra("rate_data", "user_rate")


    ##### Calculate sums for the user_rate and room_rate streams
    # Data points to sum
    sum_time_window = 20

    # Selects all data points in window, if less than limit, calcs sum based on available average
    def filter_list(points):
      max_time = max(points)[0]
      valid_points = [(point[1]) for point in points if (max_time - point[0]) < sum_time_window]

      return(valid_points)

    ### Find the max time for each user in the past xxx time and sum the latest sum_time_window points
    user_rate_values = combined_info.map(lambda x: (x[0][0], (x[0][1], x[1][1])))

    # Calculate user dose in sliding window
    windowed_user_rate = user_rate_values.groupByKeyAndWindow(window_length, batch_length).map(lambda x : (x[0], list(x[1]))).\
                         map(lambda x: {"user_id": x[0], "timestamp": max(x[1])[0], "sum_rate": costum_add(list(filter_list(x[1])), sum_time_window)})
    windowed_user_rate.saveToCassandra("rate_data", "user_sum")

    # Calculate room dose in sliding window
    windowed_room_rate = room_rate_gen.groupByKeyAndWindow(window_length, batch_length).map(lambda x : (x[0], list(x[1]))).\
                         map(lambda x: {"room_id": x[0], "timestamp": max(x[1])[0], "sum_rate": costum_add(list(filter_list(x[1])), sum_time_window)})
    windowed_room_rate.saveToCassandra("rate_data", "room_sum")


    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
  main()