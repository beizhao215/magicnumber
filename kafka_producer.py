import random
import sys
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from time import sleep
import json



class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    __base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
    __decodemap = { }
    for i in range(len(__base32)):
        __decodemap[__base32[i]] = i
    del i
    def encode(self, latitude, longitude, precision=12):
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
                geohash += self.__base32[ch]
                bit = 0
                ch = 0
        return ''.join(geohash)

    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            time_field = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fare_field = round(random.uniform(5, 100),2)
            lon_field = round(random.uniform(-73.8, -74.05),5)  # Random float
            lat_field = round(random.uniform(40.6, 41.0),5)  # Random float
            passenger_id = random.randint(1,63698)
            geohash = self.encode(lat_field,lon_field,6)
            message_info = {}
            message_info['dropoff_time'] = time_field
            message_info['fare'] = fare_field
            message_info['dropoff_lon'] = lon_field
            message_info['dropoff_lat'] = lat_field
            message_info['passenger_id'] = passenger_id
            message_info['geo_hash'] = geohash
            message_info = json.dumps(message_info)
            print message_info
            self.producer.send_messages('tripstopic', source_symbol, message_info)
            sleep(0.01)
            msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
