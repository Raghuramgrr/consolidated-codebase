


from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.sql.types import StructType
import gc
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, concat_ws
import boto3
import json

#sc = SparkContext.getOrCreate()
inputs = json.load(open('params_cluster.json'))
#inputs = json.load(open('params_cluster.json'))
#inputs = json.load(open(sys.argv[1]))

# =============================================================================
# inputs = {
#         
# "dataset":
# {
# "type":".csv",
#  "prefix":"clickstream_clustering/daimler_kmean_analysis", 
#  "folder": "clickstream_clustering/", 
#  "outputfolder":"analysis_outputs/",
#  "outputbucket":"qadams",
#  "bucket": "daimler-phase1"}, 
#  "master_node": "local", 
#  "filters": [{"index": 369, 
#  "values": ["Registration"]},
# {
# "index": 370, 
# "values": ["Failed", 
# "Request Success"]}],
# "file": "output.csv", 
# "application_name":"event_count", 
# "datetype": "month", 
# "daterange": ["2016-01-01", "2017-05-05"],
# "unique": True,
# "session_time": [23], 
# "events": [369, 370],
# "session_id": [956, 957, 962],
# "update-id":"a123dedfgh"
# }
# 
# =============================================================================

        



OUTPUT_DATA_FOLDER=inputs["dataset"]['outputfolder']
INPUT_DATA_BUCKET=inputs["dataset"]['bucket']
INPUT_DATA_FOLDER=inputs["dataset"]['folder']
INPUT_DATA_PREFIX=inputs["dataset"]['prefix']
INPUT_DATA_TYPE =inputs["dataset"]['type']
print INPUT_DATA_BUCKET,INPUT_DATA_FOLDER
# =============================================================================
# EVENTS = inputs["events"]
# FILTERS = inputs["filters"]
# UNIQUE = inputs["unique"] 
# DATERANGE = inputs["daterange"] 
# DATETYPE= inputs["datetype"] 
# SESSION_ID=inputs["session_id"] 
# SESSION_TIME= inputs["session_time"] 
# UPDATE_ID = inputs["update-id"]
# =============================================================================
HEADERS={}
IS_INIT=False
class CreateDF:

    
    def list():
        client = boto3.client('s3')
        list_objects=client.list_objects(Bucket=INPUT_DATA_BUCKET, Prefix=INPUT_DATA_PREFIX, Delimiter='/')
        list_objects = list_objects["Contents"]
        objects = []
        for obj in list_objects:
              objects.append(obj['Key'])
        return objects
    
    
    def __init__(self, file, appname, dataframe= None):
        
       # self.sc = SparkContext.stop(appName=appname)
        self.sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(self.sc)
        if dataframe == None:
            self.dataframe = sqlContext.createDataFrame(self.sc.emptyRDD(), StructType([]))
        else:
            self.dataframe = dataframe
        self.file = file
        
    def csv2data(self,donotvectorize):    
        lists=self.list_fileNames()
        #print lists
        csvFiles=[]
        s3 = boto3.client('s3')
        for l in lists:
               csvFiles.append(l.encode('utf-8').split('/')[1])

                
        for c in csvFiles:
                print c
                s3.download_file(INPUT_DATA_BUCKET,INPUT_DATA_FOLDER+c, "knn"+INPUT_DATA_TYPE)
        #datas = CreateDF.sc.wholeTextFiles("/home/ab/pyspark/knn"+INPUT_DATA_TYPE)
        #sessions = datas
        #print sessions.first()
        df = self.ReturnDataframe("/home/ab/pyspark/knn"+INPUT_DATA_TYPE)
        columns = df.columns
        columnlength = len(columns)
        print columnlength
        self.vectorize = list(set(self.dataframe.columns)-set(donotvectorize))
        self.dataframe = self.dataframe.withColumn("Features_Joined", concat_ws('_',*self.vectorize))
        self.vectors = [i+"_index" for i in self.vectorize]
        df = self.StringtoVector(self.vectorize)
        y = self.dataframe.select(['Features_Joined']).rdd.map(lambda x: x[0]).collect()
        self.TransformDataframe(self.vectors)

        
                
# =============================================================================
#             # Download File - only when you have to run in local host  - comment the next two lines for EMR running.
# 
#             
#             #s3n  for running in EMR Clsuter
#             
# =============================================================================
           # datas=self.sc.wholeTextFiles('s3n://'+INPUT_DATA_BUCKET+'/'+INPUT_DATA_FOLDER+'/'+c)
            #sessions = datas.flatMap(file_mapper)                                 .map(event_mapper)                                 .filter(filter_events)
# =============================================================================
#             output={ "data": [] }
#             for s in sessions.collect():
#                 output["data"].append({ "serie": s[0], "data": s[1] })
#             with open('event_unique.json', 'wb') as f:
#                 json.dump(output, f)
#                 #print json.dumps(output)
# 
#        # upload("event_unique.json",OUTPUT_DATA_BUCKET,format_s3_partition_key('event_unique.json'))
#         


    def list_fileNames(self):
            client = boto3.client('s3')
            list_objects=client.list_objects(Bucket=INPUT_DATA_BUCKET, Prefix=INPUT_DATA_PREFIX, Delimiter='/')
            list_objects = list_objects["Contents"]
            objects = []
            for obj in list_objects:
                  objects.append(obj['Key'])
            return objects
    

    def getInputs(self, fileName, fileNum, donotvectorize):
        df = self.dataframe
        for i in range(fileNum[0],fileNum[1]):
            if df.rdd.isEmpty():
                df = self.ReturnDataframe(fileName+str(i))
            else:
                df = df.union(self.ReturnDataframe(fileName+str(i)))
        columns =  df.columns
        columnlength = len(columns)
        # donotvectorize =  [columns[i] for i in donotvectorize]
        self.vectorize = list(set(self.dataframe.columns)-set(donotvectorize))
        self.dataframe = self.dataframe.withColumn("Features_Joined", concat_ws('_',*self.vectorize))
        self.vectors = [i+"_index" for i in self.vectorize]
        df = self.StringtoVector(self.vectorize)
        y = self.dataframe.select(['Features_Joined']).rdd.map(lambda x: x[0]).collect()
        self.TransformDataframe(self.vectors)


    def ReturnDataframe(self, file):
        print file
        rdd = self.sc.textFile(file).map(lambda line: line.split(","))
        header = rdd.first()
        self.dataframe = rdd.filter(lambda row : row != header).toDF(header)
        del rdd
        gc.collect()
        return self.dataframe


    def StringtoVector(self, vectorize):
        indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(self.dataframe) for column in vectorize ]
        pipeline = Pipeline(stages=indexers)
        self.dataframe = pipeline.fit(self.dataframe).transform(self.dataframe)
        return self.dataframe

    def TransformDataframe(self, vectors):
        assembler = VectorAssembler(inputCols=vectors,  outputCol="features")
        expr = [col(c).cast("float").alias(c) for c in assembler.getInputCols()]
        self.dataframe = assembler.transform(self.dataframe)

