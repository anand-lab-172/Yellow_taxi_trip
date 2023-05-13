# Yellow_taxi_trip
Data about yellow taxi trip

![New York City Manhattan](https://github.com/anand-lab-172/Yellow_taxi_trip/assets/74824247/9ff03536-23a4-4dd7-b73a-38da61780777)

About ```Main Code```:

* Code in ```if``` helps to run year 2009 files.
* Code in ```elif``` helps to run year 2011 and later files.
* Code in ```else``` helps to run year 2010 files.


Processing the data:

* The average price per mile traveled by the customers of taxis: Calculated and saved as ```df['avg_cost_per_miles']```
* The distribution of payment types: Calculated and saved as ```df['count_of_trips_by_payment']```
* The following custom indicator: (amount of tip + extra payment) / trip distance: Calculated and saved as ```df['fare_&_extra_per_trip']```

Processed data are stored as 20230101_yellow_taxi_kpis.json as per requested.

Reading and appending the json ouput also converting them as new chucked csv by following name 202301_yellow_taxi_kpis.csv

The ```example code``` are explained well, which helps you to understand my work. 

Please take a look for ```example code``` before getting into main code. Thank you!
