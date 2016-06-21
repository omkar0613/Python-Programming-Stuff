#!/usr/bin/env python

### Use day of week as key
### hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming-2.7.1.2.3.2.0-2950.jar -file /home/tmn491/taxi-mapV2.py -mapper /home/tmn491/taxi-mapV2.py -file /home/tmn491/taxi-red.py -reducer /home/tmn491/taxi-red.py -input /user/tmn491/trip_data.csv -output /user/tmn491/taxi-outV2

#### OUTPUT
#  Friday	2080
#  Monday	1689
#  Saturday	2049
#  Sunday	1776
#  Thursday	2506
#  Tuesday	2323
#  Wednesday	2354



import sys
from datetime import date
import datetime
import calendar

for line in sys.stdin:
    line = line.strip()
    unpacked = line.split(",")
    medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude = line.split(",")

    pickup_datetime = datetime.datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")
    day = calendar.day_name[pickup_datetime.weekday()]

    results = [day, "1"]
    print("\t".join(results))
