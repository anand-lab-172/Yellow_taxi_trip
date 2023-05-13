# Yellow_taxi_trip
Data about yellow taxi trip

About Main Code:

* Code in ```if``` helps to run 2009 files 
* Code in ```elif``` helps to run 2011 and later files 
* Code in ```else``` helps to run 2010 files


Processing the data1:

* The average price per mile traveled by the customers of taxis: Calculated and saved as ```df['avg_cost_per_miles']```
* The distribution of payment types: Calculated and saved as ```df['count_of_trips_by_payment']```
* The following custom indicator: (amount of tip + extra payment) / trip distance: Calculated and saved as ```df['fare_&_extra_per_trip']```

Processed data are stored as 20230101_yellow_taxi_kpis.json as per requested

Reading and appending the json ouput also converting them as chucked new csv by following name 202301_yellow_taxi_kpis.csv
