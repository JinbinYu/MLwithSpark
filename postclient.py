# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 16:03:58 2016

@author: hadoop
"""

import requests
import json

headers = {'content-type': 'application/json'}

#train
model = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1",
         "datasetName":"dataset1",
         "dataType":"libsvm",         
         "algoName":"LogisticRegression",
         "algoPara":{
             "maxIter":10,
             "regParam":0.1
         }
        }
#model = json.dumps(info)
r = requests.post("http://localhost:5000/model/train",
                  headers=headers,
                  data=json.dumps(model))
                  
print r.text

#query
query = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1"}

r = requests.post("http://localhost:5000/model/query",
                  headers=headers,
                  data=json.dumps(query))
                  
print r.text


#predict
predict = {"userName":"test",
         "password":"test",
         "modelName":"mylrmodel1",
         "datasetName":"dataset1",
         "outputName":"prediction1"}

r = requests.post("http://localhost:5000/model/prediction",
                  headers=headers,
                  data=json.dumps(predict))

print r.text

"""
#upload
upload = {"userName":"test",
         "password":"test",
         "datasetName":"dataset1",
         "dataType":"libsvm",
         "dataPath":"sample_libsvm_data.txt"}

r = requests.post("http://localhost:5000/dataset/upload",
                  headers=headers,
                  data=json.dumps(upload))       

print r.text
"""
