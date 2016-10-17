from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import pyspark_cassandra, sys
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import Window
from pyspark import StorageLevel
from datetime import timedelta
import json

from pyspark.sql.types import DoubleType, TimestampType, StringType
from datetime import datetime
from pyspark.sql.functions import date_format, sum, avg, udf
from functools import reduce  
from pyspark.sql import DataFrame


from math import log10

#  Note: the alphabet in geohash differs from the common base32
#  alphabet described in IETF's RFC 4648
#  (http://tools.ietf.org/html/rfc4648)
__base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
__decodemap = { }
for i in range(len(__base32)):
    __decodemap[__base32[i]] = i
del i

def decode_exactly(geohash):
    """
    Decode the geohash to its exact values, including the error
    margins of the result.  Returns four float values: latitude,
    longitude, the plus/minus error for latitude (as a positive
    number) and the plus/minus error for longitude (as a positive
    number).
    """
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    lat_err, lon_err = 90.0, 180.0
    is_even = True
    for c in geohash:
        cd = __decodemap[c]
        for mask in [16, 8, 4, 2, 1]:
            if is_even: # adds longitude info
                lon_err /= 2
                if cd & mask:
                    lon_interval = ((lon_interval[0]+lon_interval[1])/2, lon_interval[1])
                else:
                    lon_interval = (lon_interval[0], (lon_interval[0]+lon_interval[1])/2)
            else:      # adds latitude info
                lat_err /= 2
                if cd & mask:
                    lat_interval = ((lat_interval[0]+lat_interval[1])/2, lat_interval[1])
                else:
                    lat_interval = (lat_interval[0], (lat_interval[0]+lat_interval[1])/2)
            is_even = not is_even
    lat = (lat_interval[0] + lat_interval[1]) / 2
    lon = (lon_interval[0] + lon_interval[1]) / 2
    return lat, lon, lat_err, lon_err


def main():
    batch_length = 10

    sc = CassandraSparkContext(appName="magicNumberStreaming")
    hvu_list = []
    hvu_rdd = sc.cassandraTable("magic_number", "user_spend").select("passenger_id").where("date='2016-06-01'", "x").limit(1000).collect()
    for each in hvu_rdd:
        hvu_list.append(each["passenger_id"])

    ssc = StreamingContext(sc, batch_length)
    ssc.checkpoint("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/usr/stream_checkpoint")

    topic = 'tripstopic'
    # Specify all the nodes you are running Kafka on
    kafkaBrokers = {"metadata.broker.list": "54.70.249.2:9092, 52.27.243.234:9092, 54.69.226.73:9092, 54.68.152.133:9092"}

    trips_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)

    def process(rdd):
        hvu_trips = rdd.filter(lambda row: json.loads(row[1])["passenger_id"] in hvu_list)
        geohash_group = hvu_trips.map(lambda row: (json.loads(row[1])["geo_hash"],1))
        geohash_group_count = geohash_group.reduceByKey(lambda a, b: a + b)
        latlon_group_count = geohash_group_count.map(lambda row: (str(decode_exactly(str(row[0]))[0:2])[1:-1], row[1]))
        print(hvu_trips.take(1))
        #print(latlon_group_count.collect())
        latlon_group_count.saveToCassandra("magic_number","location_now",ttl=timedelta(seconds=10),)

    trips_data.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
