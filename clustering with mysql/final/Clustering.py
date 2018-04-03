
# coding: utf-8

# In[5]:

import MySQLdb
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import six
from sklearn.cross_validation import train_test_split
import itertools
from operator import itemgetter
import collections 
from sklearn.cluster import KMeans 
import matplotlib.pylab as plt
import time
import traceback
import datetime
from datetime import datetime


# In[6]:

def insertClusters(df_month,freq,mysql_cn, execution_date):
    mysql_cn_write =mysql_cn
    parameters = df_month.to_dict(orient='records')
    x = mysql_cn_write.cursor()
    df=df_month
    try:
        for P in range(0,len(freq)):
            x.execute("""INSERT INTO CLUSTERS(ALGORITHM_ID, CLUSTER_GROUP,FREQUENCY_CENTROID,AMOUNT_CENTROID,TIME_STAMP) VALUES(%s,%s,%s,%s,%s)""",(1, P,round(freq[P][0],2),round(freq[P][1],2),execution_date))
        mysql_cn_write.commit()
    except Exception as e:
        print 'insert fail',e
        mysql_cn_write.rollback()


# In[7]:

def insertCustClusters(df_month,mysql_c,m):
    mysql_cn_write =mysql_cn
    parameters = df_month.to_dict(orient='records')  
    x = mysql_cn_write.cursor()
    df=df_month
    df_cid=pd.read_sql("select ID,CLUSTER_GROUP from CLUSTERS where TIME_STAMP='"+str(m)+"'",con=mysql_cn_write)
    IDS = df_cid.to_dict(orient='records')
    df_month["CUST"] = df_month.index
    
    try:
        for P in df_month.values:
            for ID in IDS:
                if int(P[3]) == int(ID['CLUSTER_GROUP']):
                    x.execute("""INSERT INTO CUSTOMER_CLUSTERS(CUST_ID,CLUSTER_ID,FREQUENCY,AMOUNT) VALUES(%s,%s ,%s,%s)""",(int(P[4]),ID['ID'],round(P[0],2),round(P[1],2)))   
        mysql_cn_write.commit()
    except Exception as e:
        print 'insert fail',e
        mysql_cn_write.rollback()
    


# In[8]:

from sklearn import preprocessing
min_max_scaler = preprocessing.MinMaxScaler()
from scipy.spatial.distance import cdist

def kmeans_bymonth(df_mysql_freq,k,m,mysql_cn):
    clus_train = df_mysql_freq.as_matrix()
    clus_train=min_max_scaler.fit_transform(clus_train)
    cust_ids =  clus_train[:,0]
    cust_ids = min_max_scaler.fit_transform(cust_ids)
    clus_train = df_mysql_freq.drop('AVG(amount)', axis=1)
    model=KMeans(n_clusters=k)
    model.fit(clus_train)
    freq_center = model.cluster_centers_
    labels=model.labels_
    clusassign=model.predict(clus_train)
    results = pd.DataFrame(data=labels, columns=['cluster'])
    results = results.groupby(['cluster']).size().reset_index(name='Mean_Count')
    df_mysql_freq['CLUSTER-ID'] = Series(clusassign, index=df_mysql_freq.index)
    execution_date = str(datetime.today()).split(".")[0]
    insertClusters(df_mysql_freq,freq_center,mysql_cn,execution_date)
    insertCustClusters(df_mysql_freq,mysql_cn,execution_date)
    return df_mysql_freq

    


# In[13]:

#Extract data from the mysql table
#store in pandas - df


from sklearn.cluster import KMeans
mysql_cn= MySQLdb.connect(host='localhost', 
                port=3306,user='root', passwd='122', 
                db='cluster')

df_mysql_freq=pd.read_sql('SELECT CUST_ID, AVG(freq), AVG(amount),tmonth from (SELECT CUST_ID, DATE_FORMAT(COM_DELIVERDATE, "%Y/%m") as tmonth, COUNT(*) as freq, SUM(BILLING_COMMAND)/count(*) as amount FROM COMMAND_POST_PROCESSED where COM_DELIVERDATE>= DATE(NOW() - INTERVAL 6 MONTH) GROUP BY CUST_ID, tmonth order by CUST_ID, tmonth)T where CUST_ID != 2750 and CUST_ID != 1 and CUST_ID != 4188  group by CUST_ID,tmonth',con=mysql_cn)
df_mysql_freq.set_index(['CUST_ID'])
df_month=[]
values = df_mysql_freq['AVG(freq)'] * df_mysql_freq['AVG(amount)']
df_mysql_freq['AVG(weight)'] = Series(values, index=df_mysql_freq.index)
df_mysql_freq['tmonth'] = pd.to_datetime(df_mysql_freq['tmonth'],yearfirst=True,format='%Y/%m', errors='ignore')

#GROUP BY CUST_ID
df_mysql_freq = df_mysql_freq.groupby(['CUST_ID']).mean()
df_month =kmeans_bymonth(df_mysql_freq,4,datetime.today(),mysql_cn)
print df_month


# In[5]:

def plot_everyk(clust_train,labels,k):
    plt.scatter(x=clust_train.ix[:,1], y=clust_train.ix[:,0], c=labels,)
    plt.xlabel('var 1')
    plt.ylabel('var 2')
    plt.title('scatter plot for %d Clusters'%k)
    #plt.show()



# In[16]:

valid_clusters =[442,495,511,540,580,616,767,802,1192,1262,1300,1404,1461,1526,1584,1680,1755,1801,1939,2099,2327,2454,2506,2606,2607,2801,2981,3617,3650,3785,4291,4808]
list_custid=[]
groups = df_month.groupby(['CLUSTER-ID','CUST_ID'])
parameters = df_mysql_freq.to_dict(orient='records')
for custid,cluster in groups:
    #print custid
    #print groups.size()
    if custid[1] in valid_clusters:
        #print custid[0],"\t",custid[1],"\t",cluster['AVG(weight)']
        list_custid.append(custid[0])
pd.Series(list_custid).value_counts()


# In[ ]:



