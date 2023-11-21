## Run this file to auto deploy the model, run a job, and deploy the application

### Install the requirements
!bash cdsw-build.sh
!pip3 install dash

# Create the directories and upload data

# build the project
from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")


HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# set the storage variable to the default location
try : 
  s3_bucket=os.environ["STORAGE"]
except:
  tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
  root = tree.getroot()
    
  for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        s3_bucket = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]
  storage_environment_params = {"STORAGE":s3_bucket}
  storage_environment = cml.create_environment_variable(storage_environment_params)
  os.environ["STORAGE"] = s3_bucket
  
# Get User Details
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], 
            "username": user_details["username"],
            "name": user_details["name"],
            "type": user_details["type"],
            "html_url": user_details["html_url"],
            "url": user_details["url"]
            }

# Get Project Details
project_details = cml.get_project({})
project_id = project_details["id"]

# Get Default Engine Details
default_engine_details = cml.get_default_engine({})
default_engine_image_id = default_engine_details["id"]

# Create Model
example_model_input = {
"ACCOUNT_ID": 1,
"V1": "-0.966271711572087",
"V2": "-0.185226008082898",
"V3": "1.79299333957872",
"V4": "-0.863291275036453",
"V5": "-0.0103088796030823",
"V6": "1.24720316752486",
"V7": "0.23760893977178",
"V8": "0.377435874652262",
"V9": "-1.38702406270197",
"V10": "-0.0549519224713749",
"V11": "-0.226487263835401",
"V12": "0.178228225877303",
"V13": "0.507756869957169",
"V14": "-0.28792374549456",
"V15": "-0.631418117709045",
"V16": "-1.0596472454325",
"V17": "-0.684092786345479",
"V18": "1.96577500349538",
"V19": "-1.2326219700892",
"V20": "-0.208037781160366",
"V21": "-0.108300452035545",
"V22": "0.00527359678253453",
"V23": "-0.190320518742841",
"V24": "-1.17557533186321",
"V25": "0.647376034602038",
"V26": "-0.221928844458407",
"V27": "0.0627228487293033",
"V28": "0.0614576285006353",
"CLASS": "0",
"RESULT": ""
}

create_model_params = {
    "projectId": project_id,
    "runtimeId": 82, #Change this to your workspace runtime ID with Python 3.7 and Spark Enabled
    "name": "Fraud Detection " + run_time_suffix,
    "description": "Fraud Detection",
    "visibility": "private",
    "targetFilePath": "99_model.py",
    "targetFunctionName": "predict",
    "engineImageId": default_engine_image_id,
    "kernel": "python3",
    "examples": [
        {
            "request": example_model_input,
            "response": {}
        }],
    "cpuMillicores": 1000,
    "memoryMb": 2048,
    "nvidiaGPUs": 0,
    "authEnabled": False,
    "replicationPolicy": {"type": "fixed", "numReplicas": 1},
    "environment": {}}

new_model_details = cml.create_model(create_model_params)
access_key = new_model_details["accessKey"]  # todo check for bad response
model_id = new_model_details["id"]

print("New model created with access key", access_key)

#Wait for the model to deploy.
is_deployed = False
while is_deployed == False:
  model = cml.get_model({"id": str(new_model_details["id"]), "latestModelDeployment": True, "latestModelBuild": True})
  if model["latestModelDeployment"]["status"] == 'deployed':
    print("Model is deployed")
    break
  else:
    print ("Deploying Model.....")
    time.sleep(10)