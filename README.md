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

[IPython Notebook with the information of the project](https://github.com/ricardoues/data-engineering-capstone-project/blob/master/capstone_project.ipynb)

[ETL Pipeline](https://github.com/ricardoues/data-engineering-capstone-project/blob/master/etl.py)



### How to run the code

* Sign in for AWS services, go to Amazon EMR Console
* Select "Clusters" in the menu on the left, and click the "Create cluster" button.
* Release: emr-5.20.0
* Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
* Instance type: m4.xlarge
* Number of instance: 2

Also you can run the following code in AWS CLI instead of the above instructions:

```bash
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin --ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-b59d00b9","EmrManagedSlaveSecurityGroup":"sg-08b935a3e2ebc1dc8","EmrManagedMasterSecurityGroup":"sg-0445b447732160e85"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.20.0 --log-uri 's3n://aws-logs-637150515554-us-east-1/elasticmapreduce/' --name 'spark-cluster' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master Instance Group"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{}}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
```

I suggest to proceed with an EC2 key pair. Wait until the cluster has the following status: Waiting before moving on to the next step. After that connect to the master node using SSH and run the following commands in the terminal: 

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
