import csv
import time
from random import randint




filename = "data/trips_data/yellow_tripdata_2016-05.csv"    ## The original csv file
out_filename = "data/trips_data/out_yellow_tripdata_2016-05.csv"   ## the file you want to save with passenger_id

green_or_yellow = 0   # 1 for green, 0 for yellow taxi

start_time = time.time()

def process_green(line):

    pickup_time = line[1]
    dropoff_time = line[2]
    pickup_lon = float(line[5])
    pickup_lat = float(line[6])
    dropoff_lon = float(line[7])
    dropoff_lat = float(line[8])
    passenger_count = int(line[9])
    trip_distance = float(line[10])
    fare = float(line[11])
    extra = float(line[12])
    mta_tax = float(line[13])
    tip_amount = float(line[14])
    tolls_amount = float(line[15])
    improvement_fee = float(line[17])
    total_amount = float(line[18])
    pickup_location = str(pickup_lat) + ', ' + str(pickup_lon)
    dropoff_location = str(dropoff_lat) + ', ' + str(dropoff_lon)
    passenger_id = randint(1,63698)


    processed_row = [pickup_time, dropoff_time, pickup_location, pickup_lon, pickup_lat, dropoff_location, dropoff_lon, dropoff_lat,
                     passenger_count, trip_distance, fare, extra, mta_tax, tip_amount, tolls_amount,
                     improvement_fee, total_amount, passenger_id]
    return processed_row

def process_yellow(line):

    pickup_time = line[1]
    dropoff_time = line[2]
    pickup_lon = float(line[5])
    pickup_lat = float(line[6])
    dropoff_lon = float(line[9])
    dropoff_lat = float(line[10])
    passenger_count = int(line[3])
    trip_distance = float(line[4])
    fare = float(line[12])
    extra = float(line[13])
    mta_tax = float(line[14])
    tip_amount = float(line[15])
    tolls_amount = float(line[16])
    improvement_fee = float(line[17])
    total_amount = float(line[18])
    pickup_location = str(pickup_lat) + ', ' + str(pickup_lon)
    dropoff_location = str(dropoff_lat) + ', ' + str(dropoff_lon)
    passenger_id = randint(1,63698)

    processed_row = [pickup_time, dropoff_time, pickup_location, pickup_lon, pickup_lat, dropoff_location, dropoff_lon, dropoff_lat,
                     passenger_count, trip_distance, fare, extra, mta_tax, tip_amount, tolls_amount,
                     improvement_fee, total_amount, passenger_id]
    return processed_row




if __name__ == "__main__":

    with open(filename, 'rb') as csvFile:
        with open(out_filename, 'wb') as outFile:   # 'a' is append, 'wb' is overwrite
            r = csv.reader(csvFile, delimiter=',', quotechar='"')
            w = csv.writer(outFile, delimiter=',', quotechar='"')
            counter = 0
            header_row = ["pickup_time", "dropoff_time", "pickup_location", "pickup_lon", "pickup_lat",
                          "dropoff_location", "dropoff_lon", "dropoff_lat", "passenger_count", "trip_distance",
                          "fare", "extra", "mta_tax", "tip_amount", "tolls_amount",
                          "improvement_fee", "total_amount", "passenger_id"]
            w.writerow(header_row)
            for line in r:
                counter += 1
                if len(line) > 0 and counter > 2:
                    if green_or_yellow == 1:
                        processed_line = process_green(line)
                    else:
                        processed_line = process_yellow(line)
                    if counter % 100000 == 0:
                        print(counter)
                    w.writerow(processed_line)







    print("--- %s seconds ---" % (time.time() - start_time))

