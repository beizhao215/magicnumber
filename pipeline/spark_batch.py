from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, TimestampType, StringType
from datetime import datetime
from pyspark.sql.functions import date_format, sum, avg
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
import pyspark_cassandra
from geohash import decode_exactly, decode, encode



def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

sc = SparkContext(appName="magicNumberBatch")
sqlContext = SQLContext(sc)
gdf1601 = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/green_201601.json")
ydf1601 = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/yellow_201601.json")

df = unionAll(gdf1601, ydf1601)
sqlContext.registerDataFrameAsTable(df, "trips_table")
df2 = sqlContext.sql("SELECT * FROM trips_table WHERE dropoff_lat>38 AND dropoff_lat < 45 AND dropoff_lat is not null AND dropoff_lon < -70 AND dropoff_lon > -78 AND total_amount>0")

def float_to_five_digit(x):
    return round(x, 5)

udf_string_to_timestampType =  udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), TimestampType())
udf_latlon2geohash = udf(lambda lat,lon: encode(lat,lon,7), StringType())
df3 = df2.withColumn("dropoff_geohash", udf_latlon2geohash(df2['dropoff_lat'],df2['dropoff_lon']))
df4 = df3.withColumn("dropoff_time_timestamptype", udf_string_to_timestampType(df3['dropoff_time']))

df5 = df4.select(df4["dropoff_time_timestamptype"],df4["dropoff_geohash"],df4["total_amount"],df4["passenger_id"])
df6 = df5.groupby(date_format("dropoff_time_timestamptype", 'yyy-MM-dd').alias('date'),"passenger_id").agg(sum('total_amount').alias('sum_total_amount'))
sqlContext.registerDataFrameAsTable(df6, "dayidsum_table")


fbdf1 = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/facebook_network.json")
fbdf2 = sqlContext.read.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/facebook_network2.json")

fbdf = unionAll(fbdf1, fbdf2)
#fbrdd = fbdf.rdd
#mapfbrdd = fbrdd.map(list)
#mapfbrdd2 = mapfbrdd.map(lambda row: (row[1],row[0]))

#mapfbrdd2.saveToCassandra("magic_number","facebook_network")

sqlContext.registerDataFrameAsTable(fbdf, "fb_table")

df7 = df6.join(fbdf,df6.passenger_id==fbdf.self_id).drop(fbdf.self_id)
#df7 = df7.selectExpr("date as date7", "passenger_id", "sum_total_amount", "friend_id")

sqlContext.registerDataFrameAsTable(df7, "dayfriendsum_table")
#sqlContext.cacheTable('dayfriendsum_table')

cond = [df6.passenger_id==df7.friend_id]
df8 = df7.join(df6, cond)
sumdf = sqlContext.sql("SELECT d.date, f.passenger_id, f.sum_total_amount as passenger_amount, f.friend_id, d.sum_total_amount as friend_amount FROM dayfriendsum_table as f JOIN dayidsum_table as d WHERE d.date=f.date AND d.passenger_id=f.friend_id ORDER BY d.date,f.passenger_id")


sqlContext.registerDataFrameAsTable(sumdf, "sumdf_table")

dailysumdf = sqlContext.sql("SELECT date, passenger_id, ROUND(first(passenger_amount),2) as self_spend, ROUND(SUM(friend_amount),2) as friend_spend FROM sumdf_table GROUP BY date, passenger_id")
#dailysumdf.write.json("hdfs://ec2-54-69-226-73.us-west-2.compute.amazonaws.com:9000/user/201604_out.json")
#dailysumdf.cache()
dailysumrdd = dailysumdf.rdd
maprdd = dailysumrdd.map(list)
user_spend_rdd = maprdd.map(lambda row: (str(row[0]),row[3],row[1], row[2])) # date | friend_spend | passenger_id | self_spend


user_spend_rdd2 = maprdd.map(lambda row: (row[1],str(row[0]),row[3],row[2])) # passenger_id | date       | friend_spend | self_spend

user_spend_rdd.saveToCassandra("magic_number","user_spend")   # user_spend: date | friend_spend | passenger_id | self_spend


user_spend_rdd2.saveToCassandra("magic_number","user_spend2")   # user_spend2: passenger_id | date       | friend_spend | self_spend

