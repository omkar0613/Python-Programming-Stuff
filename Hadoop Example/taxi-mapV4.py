#!/usr/bin/env python

### Use day & time as key in small data set
### hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming-2.7.1.2.3.2.0-2950.jar -file /home/tmn491/taxi-mapV4.py -mapper /home/tmn491/taxi-mapV4.py -file /home/tmn491/taxi-red.py -reducer /home/tmn491/taxi-red.py -input /user/tmn491/trip_data.csv -output /user/tmn491/taxi-outV4

### OUTPUT
#  Tuesday 8	125
#  Tuesday 9	116
#  Wednesday 0	54
#  Wednesday 1	33
#  Wednesday 10	115


import sys
from datetime import date
import datetime
import calendar

for line in sys.stdin:
    line = line.strip()
    unpacked = line.split(",")
    medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude = line.split(",")

    pickup_datetime = datetime.datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')
    day = calendar.day_name[pickup_datetime.weekday()]
    hours = str(pickup_datetime.hour)
    day_time = str(day+" "+hours)



    results = [day_time, "1"]
    print("\t".join(results))
