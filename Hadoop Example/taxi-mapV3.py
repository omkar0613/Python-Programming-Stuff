#!/usr/bin/env python

### Use hours as key
### hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming-2.7.1.2.3.2.0-2950.jar -file /home/tmn491/taxi-mapV3.py -mapper /home/tmn491/taxi-mapV3.py -file /home/tmn491/taxi-red.py -reducer /home/tmn491/taxi-red.py -input /user/tmn491/trip_data.csv -output /user/tmn491/taxi-outV3

### OUTPUT
#  0	531
#  1	400
#  10	656
#  11	676
#  12	725


import sys
from datetime import date
import datetime
import calendar

for line in sys.stdin:
    line = line.strip()
    unpacked = line.split(",")
    medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude = line.split(",")

    pickup_datetime = datetime.datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')
    hours = str(pickup_datetime.hour)

    results = [hours, "1"]
    print("\t".join(results))
