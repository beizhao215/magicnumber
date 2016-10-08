from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import pyspark_cassandra, sys
from pyspark.sql import Window
from pyspark import StorageLevel
from datetime import timedelta
import json

from pyspark.sql.types import DoubleType, TimestampType, StringType
from datetime import datetime
from pyspark.sql.functions import date_format, sum, avg, udf
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def main():
    # Kafka and Spark Streaming specific vars
    batch_length = 10
    #window_length = 50

    sc = SparkContext(appName="magicNumberStreaming")

    ssc = StreamingContext(sc, batch_length)
    ssc.checkpoint("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/usr/stream_checkpoint")

    #zkQuorum, topic = sys.argv[1:]
    topic = 'tripstopic'
    # Specify all the nodes you are running Kafka on
    kafkaBrokers = {"metadata.broker.list": "54.70.249.2:9092, 52.27.243.234:9092, 54.69.226.73:9092, 54.68.152.133:9092"}

    # Get the sensor and location data streams - they have separate Kafka topics
    trips_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)

    hvu_list = range(1,10000)

    def process(rdd):
        hvu_trips = rdd.filter(lambda row: json.loads(row[1])["passenger_id"] in hvu_list)
        geohash_group = hvu_trips.map(lambda row: (json.loads(row[1])["geo_hash"],1))
        geohash_group_count = geohash_group.reduceByKey(lambda a, b: a + b)
        print(hvu_trips.take(1))
        # print(geohash_group.collect())
        geohash_group_count.saveToCassandra("magic_number","location_now",ttl=timedelta(seconds=10),)


    trips_data.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()





#raw_loc = raw_data_tojson(loc_data)




if __name__ == '__main__':
    main()