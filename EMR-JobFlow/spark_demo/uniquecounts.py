
# coding: utf-8

# In[1]:


import os
import sys

#RDD
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql .types import *
#%matplotlib inline

import spark_rdd


# In[2]:


CSV="daimler_unique_counts2017-12-01.csv"
CUST_ID_COLUMN_NAME="304"


# In[ ]:


#run p4j server
#port=25333
# run only once
#gateway = JavaGateway()
#gateway.launch_gateway(port=25333, jarpath="/usr/local/share/py4j/py4j0.10.4.jar")


# In[ ]:


#SUBMIT_ARGS = "--packages mysql:mysql-connector-java:5.1.39 pyspark-shell"
#os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
#sc = SparkContext("local", "daimler_poc")
#type(sc)


# In[3]:


def get_rdd_df(CSV):
    # Get rdd from csv and create header list
    rdd_object = spark_rdd.rdd_handle(CSV)
    rdd_array = rdd_object.get_rdd_array()
    header = rdd_array.first()
    # Check if spark running, retrieve context, bug fix for toDF call
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    # Get data frame
    rdd = rdd_array.filter(lambda row : row != header)
    if hasattr(rdd, "toDF"):
        rdd_df = rdd.toDF(header)       
        return rdd_df


# In[4]:


# Get distinct column values list
dataframe = get_rdd_df(CSV)
df_ID_distinct = dataframe.select(CUST_ID_COLUMN_NAME).distinct()
distinct_IDs = df_ID_distinct.collect()


# In[5]:


print ("Size: ", len(distinct_IDs))
print ("\n\n", distinct_IDs)

