## Part 4: Model Serving
#
# This script explains how to create and deploy Models in CML which function as a 
# REST API to serve predictions. This feature makes it very easy for a data scientist 
# to make trained models available and usable to other developers and data scientists 
# in your organization.
#
### Requirements
# Models have the same requirements as Experiments:
# - model code in a `.py` script, not a notebook
# - a `requirements.txt` file listing package dependencies
# - a `cdsw-build.sh` script containing code to install all dependencies
#
# > In addition, Models *must* be designed with one main function that takes a dictionary as its sole argument
# > and returns a single dictionary.
# > CML handles the JSON serialization and deserialization.

# In this file, there is minimal code since calculating predictions is much simpler 
# than training a machine learning model.
# This script loads and uses the `cc_scaler.pkl` file for the MinMaxScaler and the `creditcard-fraud.model` file for the 
# pytorch model. 
# When a Model API is called, CML will translate the input and returned JSON blobs to and from python dictionaries.
# Thus, the script simply loads the model we saved at the end of the last notebook,
# passes the input dictionary into the model, and returns the results as a dictionary with the following format:
#    
#    {
#       "result" : loss.item()>split_point
#    }
#
# The Model API will return this dictionary serialized as JSON.
# 
### Creating and deploying a Model
# To create a Model using our `4_model_deploy.py` script, use the following settings:
# * **Name**: Fraud Detection
# * **Description**: Deep Anomaly Detection for Fraud
# * **File**: 4_model_deploy.py
# * **Function**: predict
# * **Input**: 
# ```
#{
#  "account_id": 1,
#  "V1": "-0.966271711572087",
#  "V2": "-0.185226008082898",
#  "V3": "1.79299333957872",
#  "V4": "-0.863291275036453",
#  "V5": "-0.0103088796030823",
#  "V6": "1.24720316752486",
#  "V7": "0.23760893977178",
#  "V8": "0.377435874652262",
#  "V9": "-1.38702406270197",
#  "V10": "-0.0549519224713749",
#  "V11": "-0.226487263835401",
#  "V12": "0.178228225877303",
#  "V13": "0.507756869957169",
#  "V14": "-0.28792374549456",
#  "V15": "-0.631418117709045",
#  "V16": "-1.0596472454325",
#  "V17": "-0.684092786345479",
#  "V18": "1.96577500349538",
#  "V19": "-1.2326219700892",
#  "V20": "-0.208037781160366",
#  "V21": "-0.108300452035545",
#  "V22": "0.00527359678253453",
#  "V23": "-0.190320518742841",
#  "V24": "-1.17557533186321",
#  "V25": "0.647376034602038",
#  "V26": "-0.221928844458407",
#  "V27": "0.0627228487293033",
#  "V28": "0.0614576285006353",
#  "CLASS": "0",
#  "result": ""
#}
# ```
# * **Kernel**: Python 3
# * **Engine Profile**: 1vCPU / 2 GiB Memory (**Note:** no GPU needed for scoring)
#
# The rest can be left as is.
#
# After accepting the dialog, CML will *build* a new Docker image using `cdsw-build.sh`,
# then *assign an endpoint* for sending requests to the new Model.

## Testing the Model
# > To verify it's returning the right results in the format you expect, you can 
# > test any Model from it's *Overview* page.
#
# If you entered an *Example Input* before, it will be the default input here, 
# though you can enter your own.

## Using the Model
#
# > The *Overview* page also provides sample `curl` or Python commands for calling your Model API.
# > You can adapt these samples for other code that will call this API.
#
# This is also where you can find the full endpoint to share with other developers 
# and data scientists.
#
# **Note:** for security, you can specify 
# [Model API Keys](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-api-key-for-models.html) 
# to add authentication.

## Limitations
#
# Models do have a few limitations that are important to know:
# - re-deploying or re-building Models results in Model downtime (usually brief)
# - re-starting CML does not automatically restart active Models
# - Model logs and statistics are only preserved so long as the individual replica is active
#
# A current list of known limitations are 
# [documented here](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-models-known-issues-and-limitations.html).

from datetime import datetime
import sys
import torch
import torch.nn as nn

class autoencoder(nn.Module):
    def __init__(self,num_features):
        super(autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(num_features, 15),
            nn.ReLU(True),
            nn.Linear(15, 7))
        self.decoder = nn.Sequential(
            nn.Linear(7, 15),
            nn.ReLU(True),
            nn.Linear(15, num_features),
            nn.Tanh())

    def forward(self, x):
        x = self.encoder(x)
        x = self.decoder(x)
        return x

num_features=29
split_point=-1.207

import joblib
scaler=joblib.load('model/cc_scaler.pkl')

model = autoencoder(num_features)
model.load_state_dict(torch.load('model/creditcard-fraud.model'))
model.eval()

def predict(args):
    with torch.no_grad():
        inp=[args['ACCOUNT_ID']]+[args['V1']]+[args['V2']]+[args['V3']]+[args['V4']]+[args['V5']]+[args['V6']]+[args['V7']]+[args['V8']]+[args['V9']]+[args['V10']]+[args['V11']]+[args['V12']]+[args['V13']]+[args['V14']]+[args['V15']]+[args['V16']]+[args['V17']]+[args['V18']]+[args['V19']]+[args['V20']]+[args['V21']]+[args['V22']]+[args['V23']]+[args['V24']]+[args['V25']]+[args['V26']]+[args['V27']]+[args['V28']]
        inp=scaler.transform([inp])
        inp=torch.tensor(inp, dtype=torch.float32)
        outp=model(inp)
        loss=torch.sum((inp-outp)**2,dim=1).sqrt().log()
        res=loss.item()>split_point
        if res == True:
          res_segment = "true"
        else:
          res_segment = "false"
        return {
        "ACCOUNT_ID" : args['ACCOUNT_ID'],
        "V1" : args['V1'],
        "V2" : args['V2'],
        "V3" : args['V3'],
        "V4" : args['V4'],
        "V5" : args['V5'],
        "V6" : args['V6'],
        "V7" : args['V7'],
        "V8" : args['V8'],
        "V9" : args['V9'],
        "V10" : args['V10'],
        "V11" : args['V11'],
        "V12" : args['V12'],
        "V13" : args['V13'],
        "V14" : args['V14'],
        "V15" : args['V15'],
        "V16" : args['V16'],
        "V17" : args['V17'],
        "V18" : args['V18'],
        "V19" : args['V19'],
        "V20" : args['V20'],
        "V21" : args['V21'],
        "V22" : args['V22'],
        "V23" : args['V23'],
        "V24" : args['V24'],
        "V25" : args['V25'],
        "V26" : args['V26'],
        "V27" : args['V27'],
        "V28" : args['V28'],
        "CLASS" : args['CLASS'],
        "RESULT" : res_segment
        }