#!/bin/bash

# BUCKET_NAME="s3://daimler_demo/event_count"
# Parse arguments
# s3_bucket_script="$BUCKET_NAME/script.tar.gz"
# Download compressed script tar file from S3
# aws s3 cp $s3_bucket_script script.tar.gz
# Untar file
tar zxvf /home/hadoop/script.tar.gz -C /home/hadoop/
# Install requirements for additional Python modules (uncomment if needed)
#sudo python2.7 -m pip install pandas

sudo python 2.7 -m pip install -r  /home/hadoop/arima/requirements.txt

# hadoop fs -mkdir -p /user/hadoop/tmp
# hadoop fs -put /home/hadoop/daimler_unique_counts2017-12-01.csv /user/hadoop/tmp


spark-submit /home/hadoop/Clusteringmodels.py "{'output_id': '8a80832f61feadba0161feb6bb16001f', 'name': 'Clickstream KMEAN', 'script_name': 'Clusteringmodels.py', 'dataset': {'bucket': 'daimler-phase1', 'prefix': '', 'type': '.tsv', 'folder': 'event_count/'}, 'headers': [{'session_time': ['10']}, {'session_id': ['0']}, {'feature': ['20']}, {'k': '3'}], 'master_node': 'local', 'script_location': 's3://daimlerdemotemp/daimlerdemotemp'}"
