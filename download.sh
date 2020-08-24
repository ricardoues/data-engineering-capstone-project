#!/bin/bash

echo "Downloading yellow trip data"
for i in {2..6}; 
do
    wget  https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-0$i.csv 
done


echo "Downloading green trip data"
for i in {2..6}; 
do
    wget  https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-0$i.csv 
done

echo "Downloading covid data"
wget https://raw.githubusercontent.com/nychealth/coronavirus-data/master/case-hosp-death.csv
