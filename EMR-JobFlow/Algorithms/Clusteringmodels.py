#!/usr/bin/python
# -*- coding: utf-8 -*-

from dfclass import CreateDF
from pysparkClusters import Clustering
import sys
sys.path.insert(0, '/var/lib/retailgear/scripts/')
sys.path.insert(1,'/home/ab/daimler/Raghuworkflow')
sys.path.insert(2,'/home/ab/pyspark/')

import json
import d3

arguments={        
"dataset":
{
"type":".csv",
 "prefix":"clickstream_clustering/daimler_kmean_analysis", 
 "folder": "clickstream_clustering/", 
 "outputfolder":"analysis_outputs/",
 "outputbucket":"qadams",
 "bucket": "daimler-phase1"}, 
 "master_node": "local", 
 "filters": [{"index": 369, 
 "values": ["Registration"]},
{
"index": 370, 
"values": ["Failed", 
"Request Success"]}],
"file": "output.csv", 
"application_name":"click_stream_clustering", 
"datetype": "month", 
"daterange": ["2016-01-01", "2017-05-05"],
"unique": True,
"session_time": [23], 
"events": [288,289,376,377,378],  #288,289,376,377,378
"session_id": [956, 957,315,69,39],
"update-id":"a123dedfgh"
}




#arguments = sys.argv[1]

column_ids = arguments['session_id']
column_timestamp = arguments['session_time']
donotvectorize = column_ids + column_timestamp
# arguments['prefix']
# arguments['folder']
# arguments['bucket']
# cluster_model = 
# k = 
# N = 

print "aiSH"
fileName = "clickstream_clustering"
appName="Clustering_models"
mydf = CreateDF(None, appName, None)
#df = mydf.dataframe

# column_ids = ['956','957','315','69','39']
# column_timestamp = ['23']
# donotvectorize = column_ids + column_timestamp
# fileNum = [0,5]
N = 50

#mydf.getInputs(fileName,fileNum, donotvectorize)
df=mydf.csv2data(donotvectorize)
Clus = Clustering(mydf.dataframe, mydf.sc)

cluster_model = 'kmeans'
k = 10

vectors = [i+"_index" for i in mydf.vectorize]

title = 'Clusters derived from all the clicks'
description = 'Each bubble is a group of customers who have the same interests in the same group of cars. The size of the bubble indicates the number of clicks.'
source = ''

if cluster_model == 'kmeans':
	Clus.cluster_kmeans(k)
	data = Clus.PercentOut("kmeans_prediction")
	chart = d3.dbscan(title, description, data, source, options={})
elif cluster_model == 'dbscan':
	Clus.cluster_dbscan(vectors, N)
	chart = Clus.PercentOut("dbscan_prediction")
	chart = d3.dbscan(title, description, data, source, options={})
elif cluster_model == 'knn':
	y = Clus.dataframe.select(vectors[-1].split('_index')[0]).collect()
	y = [i[0] for i in y]
	Clus.cluster_knn(mydf.dataframe.select(vectors[:-1]).collect(), y)

print json.dumps([chart])

# kmeans_c = Counter(Clus.dataframe.select("kmeans_prediction").collect())
# kmeans_c = dict(kmeans_c)
# dbscan_c = Counter(Clus.dataframe.select("dbscan_prediction").collect())
# dbscan_c = dict(dbscan_c)

# for i in kmeans_c.keys():
# 	print i , kmeans_c[i]
# print "////////////////////"
# for i in dbscan_c.keys():
# 	print i , dbscan_c[i]

mydf.sc.stop()