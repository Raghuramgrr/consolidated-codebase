
# coding: utf-8

# In[2]:

import json
import boto3
import sys
s3 = boto3.resource('s3')
emr_client = boto3.client('emr')
client = boto3.client('lambda')
#data = json.loads(sys.argv[1])
data =json.load(open('params_cluster.json'))
ARGS=json.load(open('params_cluster.json'))
INPUT_DATA_BUCKET=data["dataset"]["bucket"]
INPUT_DATA_FOLDER=data["dataset"]["folder"]
OUTPUT_DATA_BUCKET="qadams"
OUTPUT_DATA_FOLDER="analysis_outputs/"
#UPDATE_ID = data["output-id"]
UPDATE_ID='8a80832f61f9b61a0161fab7d5a0003b'
SCRIPT_LOCATION = data["script_location"]
SCRIPT_NAME=data["script_name"]
#print SCRIPT_LOCATION,SCRIPT_NAME


# In[10]:

clusters = emr_client.list_clusters()
clusters = [c["Id"] for c in clusters["Clusters"] 
            if c["Status"]["State"] in ["RUNNING", "WAITING"]]
if not clusters:
    print json.dumps({"success":False,"message":"Error.No Active clusters"})
else:
    cluster_id=clusters[0]
    print json.dumps({"success":True,"Cluster-id":""cluster_id})


# In[18]:

#trigger.py '{"dataset":{"type":"csv","prefix":"","folder":"datasets/Mentorica/ITI/SERVER6","bucket":"qadams","has_headers":true},"master_node":"local","output_id":"8a80832f6252202b0162522751290028",
#"output_bucket":"qadams","output_folder":"analysis_outputs/8a80832f6252202b0162522751290028",
#"script_location":"s3://qadams/analysis_scripts/timeseries/arima.tar.gz","script_name":"arima.py","name":"ARIMA",
#"headers":{"metric_name":["4","3"],"metric_value":["7"],"date":["1"],"d":"0","p":"0","q":"1"}}'


payload='{"output_path": "%s", "update-id":"%s","SCRIPT":"%s","SCRIPT_NAME":"%s","ARGS":"%s"}' %(OUTPUT_DATA_BUCKET+"/"+OUTPUT_DATA_FOLDER,UPDATE_ID,SCRIPT_LOCATION,SCRIPT_NAME,ARGS)

d = json.loads(boto3.client('lambda').invoke(             FunctionName='EMR_Jobflow',             Payload= payload             )["Payload"].read())
#print d
if("Added step:" in d):
    print json.dumps({"success":True,"Message":"Scheduled! Your analysis has been correctly scheduled","response":d})
#trigger.py '{"dataset":{"type":"csv","prefix":"","folder":"datasets/Mentorica/ITI/SERVER6","bucket":"qadams","has_headers":true},"master_node":"local","output_id":"8a80832f6252202b0162522751290028","output_bucket":"qadams","output_folder":"analysis_outputs/8a80832f6252202b0162522751290028","script_location":"s3://qadams/analysis_scripts/timeseries/arima.tar.gz","script_name":"arima.py","name":"ARIMA","headers":{"metric_name":["4","3"],"metric_value":["7"],"date":["1"],"d":"0","p":"0","q":"1"}}'
#'{"dataset":{"type":"csv","prefix":"test_tmp.csv","folder":"event_count/","bucket":"daimler-phase1"},"master_node":"local","output_id":"ff80818161f972970161f9975fc0001d","script":"s3://daimlerdemotemp/daimlerdemotemp","name":"Clickstream KMEAN","headers":[{"feature":["10"]},{"session_id":["0"]},{"session_time":["20"]},{"k":"3"}]}'
#update_flag = wait_for_steps_completion(emr_client,cluster_id,0)


# In[ ]:

if update_flag:
    client = boto3.client('lambda')
    d = json.loads(boto3.client('lambda').invoke(
            FunctionName='workflow_postgre_update',
            Payload='{"output_path": "%s", "update-id":"%s"}' %(OUTPUT_DATA_BUCKET+"/"+OUTPUT_DATA_FOLDER,UPDATE_ID)
            )["Payload"].read())
            
print d
    
#print json.dumps({"success": True})


# In[49]:

def wait_for_steps_completion(emr_client, emr_cluster_id, max_attempts):
#max_attempts=0
    sleep_seconds = 30
    num_attempts = 0

    while True:
        response = emr_client.list_steps(
            ClusterId=cluster_id,
            StepStates=['PENDING', 'CANCEL_PENDING', 'RUNNING']
        )
        num_attempts += 1
        active_aws_emr_steps = response['Steps']
        print active_aws_emr_steps 
        if active_aws_emr_steps:
            if 0 < max_attempts <= num_attempts:
                raise Exception(
                    'Max attempts exceeded while waiting for AWS EMR steps completion. Last response:\n'
                    + json.dumps(response, indent=3, default=str)
                )
            time.sleep(sleep_seconds)
        else:
            print 'in else part'
            return True

