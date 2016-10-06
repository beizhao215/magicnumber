import random
import sys
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from time import sleep



class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
            fare_field = round(random.uniform(5, 100),2)
            lon_field = round(random.uniform(-73.8, -74.05),5)  # Random float
            lat_field = round(random.uniform(40.6, 41.0),5)  # Random float
            passenger_id = random.randint(1,63698)
            str_fmt = "source:{},dropoff_time:{},fare:{},dropoff_lon:{},dropoff_lat:{},passenger_id:{}"

            message_info = str_fmt.format(source_symbol,
                                          time_field,
                                          fare_field,
                                          lon_field,
                                          lat_field,
                                          passenger_id)
            print message_info
            self.producer.send_messages('trips', source_symbol, message_info)
            sleep(0.5)
            msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
