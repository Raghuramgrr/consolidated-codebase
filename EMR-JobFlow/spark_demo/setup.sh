#!/bin/bash

# Parse arguments
s3_bucket="daimlerdemotemp"
s3_bucket_script="$s3_bucket/script.tar.gz"

# Download compressed script tar file from S3

aws s3 cp $s3_bucket_script /home/hadoop/script.tar.gz

# Untar file
tar zxvf "/home/hadoop/script.tar.gz" -C /home/hadoop/

#python
sudo python2.7 -m pip install referer_parser


