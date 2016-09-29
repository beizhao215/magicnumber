from elasticsearch import Elasticsearch
host = 'http://ec2-52-35-205-50.us-west-2.compute.amazonaws.com:9200/'
es = Elasticsearch([host], verify_certs=True)
trips_mapping = '''
{
      "mappings": {
         "trip": {
            "properties": {
               "pickup_time": {"type": "date", "format": "yy-MM-dd HH:mm||yy-MM-dd HH:mm:ss"},
               "dropoff_time": {"type": "date", "format": "yy-MM-dd HH:mm||yy-MM-dd HH:mm:ss"},
               "pickup_location": {"type": "geo_point"},
               "pickup_lon": {
                  "type": "double"
               },
               "pickup_lat": {
                  "type": "double"
               },
               "dropoff_location": {"type": "geo_point"},
               "dropoff_lon": {
                  "type": "double"
               },
               "dropoff_lat": {
                  "type": "double"
               },
               "passenger_count": {
                  "type": "integer"
               },
               "trip_distance": {
                  "type": "float"
               },
               "fare": {
                  "type": "float"
               },
               "extra": {
                  "type": "float"
               },
               "mta_tax": {
                  "type": "float"
               },
               "tip_amount": {
                  "type": "float"
               },
               "tolls_amount": {
                  "type": "float"
               },
               "improvement_fee": {
                  "type": "float"
               },
               "total_amount": {
                  "type": "float"
               },
               "passenger_id": {
                  "type": "long"
               }
            }
         }
	}
} '''
es.indices.create(index='trips', body=trips_mapping)


socialnet_mapping = '''
{
      "mappings": {
         "connection": {
            "properties": {
               "self_id": {"type": "long"},
               "friend_id": {"type": "long"}
            }
         }
	}
} '''

es.indices.create(index='socialnet', body=socialnet_mapping)
