#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
from sklearn.cluster import DBSCAN
#import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from pyspark.sql.functions import udf, col, sum, lit
from pyspark.ml.clustering import KMeans
import time
from pyspark.sql import SQLContext, Window, Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier, NearestNeighbors
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score

from sklearn.neighbors import KNeighborsClassifier

class Clustering():
	def __init__(self, dataframe, sc):
		self.dataframe = dataframe
		self.sc =sc
		self.sqlContext = SQLContext(sc)
		self.kmeans = []

	def cluster_kmeans(self, k=22):
			# cost = np.zeros(20)
			# for k in range(2,20):
			# 	kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
			# 	model = kmeans.fit(self.dataframe.sample(False,0.1, seed=42))
			# 	cost[k] = model.computeCost(self.dataframe)

			# fig, ax = plt.subplots(1,1, figsize =(8,6))
			# ax.plot(range(2,20),cost[2:20])
			# ax.set_xlabel('k')
			# ax.set_ylabel('cost')
			# fig.show()
			# time.sleep(20)
		kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features").setPredictionCol("kmeans_prediction")
		model = kmeans.fit(self.dataframe)
		centers = model.clusterCenters()
		try:
			model.save("kmeans-model"+str(k))
		except:
			model.write().overwrite().save("kmeans-model"+str(k))
		# print("Cluster Centers: ")
		# plt.plot(centers, '-o')
		# plt.show()
		self.dataframe = model.transform(self.dataframe)


		#transformed  = model.transform(self.dataframe).select('prediction')
		# rows = transformed.collect()
		# df_pred = self.sqlContext.createDataFrame(rows)
		# df_pred.show()

		# oldColumns = data.schema.names
		# print "old", oldColumns
		# newColumns = ["name", "age"]

		# df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), data)
		# df.printSchema()
		# df.show()
	def cluster_knn(self, X, y):
		# X = self.dataframe.select(*vectors).collect()
		# neigh = NearestNeighbors(n_neighbors=1)
		# neigh.fit(X)

		X_train, X_test, y_train, y_test = train_test_split( X, y, test_size = 0.2, random_state = 100)
		neigh = KNeighborsClassifier(n_neighbors=10)
		neigh.fit(X_train, y_train) 
		y_pred = neigh.predict(X_test)
 		print "Accuracy is ", accuracy_score(y_test,y_pred)*100

 		predictions = []
 		kNearestNeighbor(X_train, y_train, X_test, predictions, 7)
 		predictions = np.asarray(predictions)
 		accuracy = accuracy_score(y_test, predictions)
		print('\nThe accuracy of our classifier is %d%%' % accuracy*100)
	#	print(neigh.predict([[1.1]]))
	#	print(neigh.predict_proba([[0.9]]))


	def plot_cluster(self, cluster, sample_matrix):
		f = lambda row: [float(x) for x in row]
		sample_matrix = map(f,sample_matrix)
		sample_matrix = StandardScaler().fit_transform(sample_matrix)
		core_samples_mask = np.zeros_like(cluster.labels_, dtype=bool)
		core_samples_mask[cluster.core_sample_indices_] = True
		labels = cluster.labels_
		# Black removed and is used for noise instead.
		unique_labels = set(labels)
		colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))
		for k, col in zip(unique_labels, colors):
			if k == -1:
				# Black used for noise.
				col = 'k'
		class_member_mask = (labels == k)  # generator comprehension 
		# X is your data matrix
		X = np.array(sample_matrix)
		xy = X[class_member_mask & core_samples_mask]
		plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col, markeredgecolor='k', markersize=14)
		xy = X[class_member_mask & ~core_samples_mask]
		plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col, markeredgecolor='k', markersize=6)

		plt.ylim([0,10]) 
		plt.xlim([0,10])
		plt.show()

	def cluster_dbscan(self, vectors, N):
		dataset_array = self.dataframe.select(*vectors).collect()
		model = DBSCAN(eps=0.12, min_samples=N).fit(dataset_array)
		db = self.sqlContext.createDataFrame([(str(l),) for l in model.labels_], ['dbscan_predict'])
		#a = self.dataframe.withColumn("row_idx", monotonically_increasing_id())
		a_columns = self.dataframe.columns + ['index']
		b_columns = db.columns + ['index']
		a = self.add_row_num(self.dataframe)
		b = self.add_row_num(db)
		self.dataframe = a.join(b, on='row_num').drop('row_num').withColumn("dbscan_prediction", (col("dbscan_predict")+1).cast('integer'))
		# try:
		# 	model.save("dbscan-model"+str(N))
		# except:
		# 	model.write().overwrite().save("dbscan-model"+str(N))

	def PercentOut(self, groupCol):
		Num_clusters = self.dataframe.select(groupCol).distinct().count()
		window = Window.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
		df = self.dataframe.groupBy("Features_Joined").pivot(groupCol).count()
		total = df.groupBy().sum().collect()[0]
		dataout = []
		out = {}
		for i in range (0, Num_clusters):
			df1 = df.where(col(str(i)).isNotNull()).select("Features_Joined",str(i)).withColumn('total',sum(col(str(i))).over(window)).withColumn('Percent',col(str(i))*100/col('total')).drop(col('total'))

			ClustData = {}
			ClustData['name'] = 'Cluster_' +str(i)
			ClustData['children'] = []
			out[i]={}
			#df1 = df.where(col(str(i)).isNotNull()).select("Features_Joined",str(i)).withColumn('Percent',col(str(i))*100/total[i])
			for colname in ["Features_Joined",str(i),'Percent']:
				out[i][colname] = df1.rdd.map(lambda x: x[colname]).collect()

			ClustData['size'] = len(out[i]['Features_Joined'])	

			for childnum in range(0, ClustData['size']):
				child = {}
				child["display"] = True
				child["name"] = out[i]['Features_Joined'][childnum]
				child['subname'] = 'Number of clicks: '+ str(out[i][str(i)][childnum])
				child['key'] = 'Percent: '+ str(round(out[i]['Percent'][childnum],2))
				child["size"] = 1
				ClustData['children'].append(child)
			dataout.append(ClustData)
		return dataout


	def count_not_null(self, c, nan_as_null=False):
		pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
		return sum(pred.cast("integer")).alias(c)

	def nullcounter(self, arr):
		print arr
		sum1 = [sum(x) for x in arr]
		print sum1	

	def add_row_num(self, df):
		df_row_num = df.rdd.zipWithIndex().toDF(['features', 'row_num'])
		df_out = df_row_num.rdd.map(lambda x : Clustering.flatten_row(x)).toDF()
		df_out.show()
		return df_out

	@staticmethod
	def flatten_row(r):
		r_ =  r.features.asDict()
		r_.update({'row_num': r.row_num})
		return Row(**r_)