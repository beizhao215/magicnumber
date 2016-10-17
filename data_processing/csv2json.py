import sys, getopt
import csv
import json

#Get Command Line Arguments
def main(argv):
    input_file = ''
    output_file = ''
    format = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:f:",["ifile=","ofile=","format="])
    except getopt.GetoptError:
        print 'csv_json.py -i <path to inputfile> -o <path to outputfile> -f <dump/pretty>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'csv_json.py -i <path to inputfile> -o <path to outputfile> -f <dump/pretty>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-o", "--ofile"):
            output_file = arg
        elif opt in ("-f", "--format"):
            format = arg
    read_csv(input_file, output_file, format)

#Read CSV File
def read_csv(file, json_file, format):
    csv_rows = []
    #action = [{ "create" : { "_type" : "connection", "_index" : "socialnet"} }]
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames

        for row in reader:
            #csv_rows.extend(action)
            # for i in range(len(title)):
            #     csv_rows.extend([{title[i]:row[title[i]])
            #
            for key, value in row.iteritems():
                do_nothing_keys = {"pickup_time", "dropoff_time", "pickup_location", "dropoff_location"}
                float_keys = {"pickup_lon", "pickup_lat", "dropoff_lon", "dropoff_lat",
                            "trip_distance", "fare", "extra", "mta_tax", "tip_amount",
                            "tolls_amount", "improvement_fee", "total_amount"}
                int_keys = {"passenger_id", "passenger_count", "self_id", "friend_id"}
                if key in do_nothing_keys:
                    row[key] = value
                elif key in float_keys:
                    row[key] = float(value)
                elif key in int_keys :
                    row[key] = int(value)




            csv_rows.extend([{title[i]:row[title[i]] for i in range(len(title))}])

        write_json(csv_rows, json_file, format)

#Convert csv data into json and write it
def write_json(data, json_file, format):
    with open(json_file, "w") as f:
        if format == "pretty":
            f.write(json.dumps(data, sort_keys=False, indent=4, separators=(',', ': '),encoding="utf-8",ensure_ascii=False))
        else:
            for each in data:
                f.write(json.dumps(each))
                f.write('\n')

if __name__ == "__main__":
   main(sys.argv[1:])