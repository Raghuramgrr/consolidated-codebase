
# coding: utf-8

# In[18]:
import sys 
import boto3
from pyspark import SparkContext, SparkConf
from datetime import timedelta
from pyspark.sql import *
from pyspark.sql .types import *
import json 
import datetime

#params = '{"dataset":{"type":".csv", "prefix":"event_count/partition", "folder": "event_count/", "bucket": "daimler-phase1"}, "master_node": "local", "filters": [{"index": 369, "values": ["Registration"]},{"index": 370, "values": ["Failed", "Request Success"]}], "file": "output.csv", "application_name":"event_count", "datetype": "month", "daterange": ["2016-01-01", "2017-05-05"], "unique": true, "session_time": [23], "events": [369, 370], "session_id": [956, 957, 962]}'
#params = open(params.json).read()
sc = SparkContext.getOrCreate()
#inputs = json.load(open('params.json'))
inputs = json.load(open(sys.argv[1]))
OUTPUT_DATA_FOLDER=inputs ["dataset"]["outputfolder"]


INPUT_DATA_BUCKET=inputs["dataset"]['bucket']
INPUT_DATA_FOLDER=inputs["dataset"]['folder']
#UNIQUE = inputs["unique"]
INPUT_DATA_PREFIX=inputs["dataset"]['prefix']
OUTPUT_DATA_BUCKET= inputs["dataset"]["outputbucket"]
INPUT_DATA_TYPE =inputs["dataset"]['type']
EVENTS = inputs["events"]
FILTERS = inputs["filters"]
UNIQUE = inputs["unique"] 
DATERANGE = inputs["daterange"] 
DATETYPE= inputs["datetype"] 
SESSION_ID=inputs["session_id"] 
SESSION_TIME= inputs["session_time"] 
UPDATE_ID = inputs["update-id"]
HEADERS = {}
IS_INIT = False
DUMMY_DATA = "upload_dummy"
print OUTPUT_DATA_BUCKET,OUTPUT_DATA_FOLDER


# In[34]:

def file_mapper(line):
    lines = line[1].split("\n")
    if not IS_INIT:
        return lines
    else:
        return lines[1:]

def event_mapper(line):
   
    line = line.split(",")

    if len(HEADERS.keys()) == 0:
        for i in range(0, len(line)):
            HEADERS[int(line[i])] = i
        return line
    if len(line) != len(HEADERS.keys()):
        return ("ERROR", [])
    HEADERSS = {962: 10, 69: 2, 61: 1, 369: 3, 370: 4, 23: 0, 376: 5, 377: 6, 378: 7, 956: 8, 957: 9}
    key = []
    value = []
    for _id in SESSION_ID:
        key.append(line[HEADERSS[_id]])
    for event in EVENTS:
        key.append(line[HEADERSS[event]])

    for filtr in FILTERS:
        value.append(line[HEADERSS[filtr["index"]]])

    for tme in SESSION_TIME:
        value.append(convert_timestamp_to_datetype(int(line[HEADERSS[tme]])))
        value.append(int(line[HEADERSS[tme]]))

    return ("_".join(key), value)
def filter_events(x):
    if x[0] == "ERROR":
        return False
    hasEvent = True
    x = x[1]
#     for i in range(0, len(FILTERS)):
#         if x[i] not in FILTERS[i]["values"]:
#             return False
#     if int(x[len(FILTERS)+1]) < daterange_to_timestamp(DATERANGE[0]) or int(x[len(FILTERS)+1]) > daterange_to_timestamp(DATERANGE[1]):
#         return False
    return True
def daterange_to_timestamp(date):
    _dt = datetime.datetime.strptime(date, '%Y-%M-%d')
    return int((_dt-datetime.datetime(1970,1,1)).total_seconds())

def convert_timestamp_to_datetype(timestamp):
    date_type = "%b %Y"
    if DATETYPE == "month":
        date_type = "%b %Y"
    if DATETYPE == "week":
        date_type = "Week %W, %Y"
    if DATETYPE == "day":
        date_type = "%Y-%M-%d"
    if DATETYPE == "weekday":
        date_type = "%A"
    if DATETYPE == "hour":
        date_type = "%Y-%M-%d %I%p"
    if DATETYPE == "hourday":
        date_type = "%I%p"
    return datetime.datetime.fromtimestamp(timestamp).strftime(date_type)

def upload(file,bucket, key):
    #print bucket,key
    s3 = boto3.resource('s3')
    data = open(file, 'rb')
    s3.Bucket(bucket).put_object(Key=key, Body=data)
    
def format_s3_partition_key(key):
    return OUTPUT_DATA_FOLDER+"/"+key+"_"+UPDATE_ID


# In[39]:

class event_count:
    
  #  params = '{"dataset":{"type":".csv", "prefix":"event_count/partition", "folder": "event_count/", "bucket": "daimler-phase1"}, "master_node": "local", "filters": [{"index": 369, "values": ["Registration"]},{"index": 370, "values": ["Failed", "Request Success"]}], "file": "output.csv", "application_name":"event_count", "datetype": "month", "daterange": ["2016-01-01", "2017-05-05"], "unique": true, "session_time": [23], "events": [369, 370], "session_id": [956, 957, 962]}'
    
    def list():
        client = boto3.client('s3')
        list_objects=client.list_objects(Bucket=INPUT_DATA_BUCKET, Prefix=INPUT_DATA_PREFIX, Delimiter='/')
        list_objects = list_objects["Contents"]
        objects = []
        for obj in list_objects:
              objects.append(obj['Key'])
        return objects
    
    def map_by_date(x):
            key = x[0].split("_")
            key = key[len(SESSION_ID):]
            key = [x[1][len(FILTERS)]]+ key
            return ("_".join(key), 1)

    def merge_keys(a,b):
        return a

    def format_result(x):
        key = x[0].split("_")
        label = key[0]
        serie = " ".join(key[1:])
        count = x[1]
        return (serie, (label, count))

 
    
    def list_fileNames(self):
        client = boto3.client('s3')
        list_objects=client.list_objects(Bucket=INPUT_DATA_BUCKET, Prefix=INPUT_DATA_PREFIX, Delimiter='/')
        list_objects = list_objects["Contents"]
        objects = []
        for obj in list_objects:
              objects.append(obj['Key'])
        return objects

    
    def csv2data(self):
        lists=self.list_fileNames()
        print lists
        csvFiles=[]
        for l in lists:
               csvFiles.append(l.encode('utf-8').split('/')[1])

                
        s3 = boto3.client('s3')
        for c in csvFiles:
            # Download File - only when you have to run in local host  - comment the next two lines for EMR running.

            #s3.download_file(INPUT_DATA_BUCKET,INPUT_DATA_FOLDER+c, "unique_count"+INPUT_DATA_TYPE)
            #datas = sc.wholeTextFiles("unique_count"+INPUT_DATA_TYPE)
            
            #s3n  for running in EMR Clsuter
            
            datas=sc.wholeTextFiles('s3n://'+INPUT_DATA_BUCKET+'/'+INPUT_DATA_FOLDER+'/'+c)
            sessions = datas.flatMap(file_mapper)                                 .map(event_mapper)                                 .filter(filter_events)
            output={ "data": [] }
            for s in sessions.collect():
                output["data"].append({ "serie": s[0], "data": s[1] })
            with open('event_unique.json', 'wb') as f:
                json.dump(output, f)
                #print json.dumps(output)

        upload("event_unique.json",OUTPUT_DATA_BUCKET,format_s3_partition_key('event_unique.json'))
    


# In[40]:

eventcount = event_count()

eventcount.csv2data()


# In[ ]:



