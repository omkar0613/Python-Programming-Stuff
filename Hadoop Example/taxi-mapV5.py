#!/usr/bin/env python

### Use day and time in full year ~19 minutes for 26 GB
### hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming-2.7.1.2.3.2.0-2950.jar -file /home/tmn491/taxi-mapV5.py -mapper /home/tmn491/taxi-mapV5.py -file /home/tmn491/taxi-red.py -reducer /home/tmn491/taxi-red.py -input /user/tmn491/2011_taxi_data.csv -output /user/tmn491/taxi-outV5


### OUTPUT
#  Tuesday 8       1305773
#  Tuesday 9       1245355
#  Wednesday 0     711262
#  Wednesday 1     435908
#  Wednesday 10    1151552


import sys
from datetime import date
import datetime
import calendar

for line in sys.stdin:
    line = line.strip()
    unpacked = line.split(",")
    vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount = line.split(",")
    #medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude = line.split(",")

    pickup_datetime = datetime.datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')
    day = calendar.day_name[pickup_datetime.weekday()]
    hours = str(pickup_datetime.hour)
    day_time = str(day+" "+hours)



    results = [day_time, "1"]
    print("\t".join(results))
