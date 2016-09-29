from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, TimestampType, StringType
from datetime import datetime
from pyspark.sql.functions import date_format, sum, avg
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

sc = SparkContext(appName="magicNumberBatch")
sqlContext = SQLContext(sc)
gdf = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/green_201606.json")
ydf = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/yellow_201606.json")

df = unionAll(gdf, ydf)

sqlContext.registerDataFrameAsTable(df, "trips_table")
df2 = sqlContext.sql("SELECT * FROM trips_table WHERE dropoff_lat>38 AND dropoff_lat < 45 AND dropoff_lat is not null AND dropoff_lon < -70 AND dropoff_lon > -78 AND total_amount>0")

def float_to_five_digit(x):
    return round(x, 5)


__base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
__decodemap = { }
for i in range(len(__base32)):
    __decodemap[__base32[i]] = i
del i
def encode(latitude, longitude, precision=12):
    """
    Encode a position given in float arguments latitude, longitude to
    a geohash which will have the character count precision.
    """
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    geohash = []
    bits = [ 16, 8, 4, 2, 1 ]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if longitude > mid:
                ch |= bits[bit]
                lon_interval = (mid, lon_interval[1])
            else:
                lon_interval = (lon_interval[0], mid)
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if latitude > mid:
                ch |= bits[bit]
                lat_interval = (mid, lat_interval[1])
            else:
                lat_interval = (lat_interval[0], mid)
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash += __base32[ch]
            bit = 0
            ch = 0
    return ''.join(geohash)



udf_string_to_timestampType =  udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), TimestampType())
udf_latlon2geohash = udf(lambda lat,lon: encode(lat,lon,7), StringType())
df3 = df2.withColumn("dropoff_geohash", udf_latlon2geohash(df2['dropoff_lat'],df2['dropoff_lon']))
df4 = df3.withColumn("dropoff_time_timestamptype", udf_string_to_timestampType(df3['dropoff_time']))

df5 = df4.select(df4["dropoff_time_timestamptype"],df4["dropoff_geohash"],df4["total_amount"],df4["passenger_id"])
df6 = df5.groupby(date_format("dropoff_time_timestamptype", 'yyy-MM-dd'),"passenger_id").agg(sum('total_amount'))
