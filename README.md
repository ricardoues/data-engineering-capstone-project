## Data Engineering Capstone Project

This is the capstone project from the 
data engineer nanodegree of udacity. 

### Introduction

The goal of this project is to create a data pipeline that provides a clean relational database that will be used for a data visualization of the number of yellow taxi trips, green taxi trips, uber taxi trips and lyft taxi from February to June 2020 along with the case count of COVID-19 in New York city .

The project follows the follow steps:

    Step 1: Scope the Project and Gather Data
    Step 2: Explore and Assess the Data
    Step 3: Define the Data Model
    Step 4: Run ETL to Model the Data
    Step 5: Complete Project Write Up


### Files 

[Capstone Project](https://github.com/ricardoues/data-engineering-capstone-project/blob/master/capstone_project.ipynb)

[ETL Pipeline](https://github.com/ricardoues/data-engineering-capstone-project/blob/master/etl.py)

[SQLite database](https://github.com/ricardoues/data-engineering-capstone-project/blob/master/taxi_covid_nyc.db?raw=true)

### How to run the code

* Sign in for AWS services, go to Amazon EMR Console
* Select "Clusters" in the menu on the left, and click the "Create cluster" button.
* Release: emr-5.20.0
* Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
* Instance type: m4.xlarge
* Number of instance: 2


We suggest to proceed with an EC2 key pair. Wait until the cluster has the following status: Waiting before moving on to the next step. After that connect to the master node using SSH and run the following commands in the terminal: 

```bash
sudo cp /etc/spark/conf/log4j.properties.template /etc/spark/conf/log4j.properties
sudo sed -i 's/log4j.rootCategory=INFO, console/log4j.rootCategory=ERROR,console/' /etc/spark/conf/log4j.properties
```

Next, we run the following commands in the terminal:

```bash
cd /etc/spark/conf/
sudo cp spark-env.sh spark-env.sh.bkp
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

Then, we install configparser and pandas python packages with the following command: 

```bash
sudo python3 -m  pip install configparser pandas
```

Finally, we will clone the repository and submit the spark script with the following commands: 

```bash
git clone https://github.com/ricardoues/data-lake.git
/usr/bin/spark-submit --verbose  --master yarn  etl.py 


The etl pipeline takes around two minutes to finish. 
