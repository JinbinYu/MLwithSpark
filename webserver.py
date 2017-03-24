# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 13:50:03 2016

@author: hadoop
"""

from flask import Flask,jsonify,request
import json
import commands
import database
import time
import os 
import subprocess
from time import strftime
import MySQLdb

app = Flask(__name__)


@app.route('/')
def index():
    
    return "Welcome"

@app.route("/command",methods=['post'])
def command():

    print request.form
    print request.form["name"]
    return "hello"

@app.route("/register",methods=["post"])
def register():
    name = request.form["name"]
    password = request.form["password"]
    
@app.route("/model/query",methods=["post"])
def queryModel():
    userName = request.json["userName"]
    password = request.json["password"]
    modelName = request.json["modelName"]
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    sql = "select * from model where userId="+userId+" and modelName="+"\""+modelName+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    if len(result) == 0:
        return jsonify({"status":-1,"info":"model: "+modelName+" does not exist"})
    
    info = {}
    info["modelName"] = result[0][1]
    info["algoName"] = result[0][2]
    info["status"] = result[0][3]
    info["time"] = result[0][5]
    info["algoPara"] = eval(result[0][6])
    info["createTime"] = result[0][7]
    info["datasetName"] = result[0][8]
    
    return jsonify(info)
    
@app.route("/model/train",methods=["post"])
def trainModel():
    
    userName = request.json["userName"]
    password = request.json["password"]
    datasetName = request.json["datasetName"]
    modelName = request.json["modelName"]
    algoName = request.json["algoName"]
    algoPara = request.json["algoPara"]  
    #dataType = request.json["dataType"]
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    #search dataPath on hdfs
    dataPath = searchDataset(userId,datasetName)
    print "dataPath: "+dataPath
    if dataPath == "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" does not exist"})
    
    #search dataType
    sql = "select dataType from dataset where userId="+userId+" and datasetName="+"\""+datasetName+"\""
    dataType = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)[0][0]
    
    
    #model path
    modelPath = searchModel(userId,modelName)
    if modelPath != "":
        return jsonify({"status":-1,"info":"model: "+modelName+" already exists"})
    modelPath = hdfs_path+"/datastudio/user/" + userName + "/model/" + modelName
    
    
    #update table model,set status to 1(running)
    #if model does not exist:insert,else:update
    sql = "select * from model where modelName="+"\""+modelName+"\""+" and userId="+ userId
                           
    if len(database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)) == 0:
        sql = "insert into model(userId,modelName,algoName,status,modelPath,algoPara,datasetName) values("+ \
                userId+",\""+modelName+"\","+"\""+algoName+"\","+"1"+",\""+modelPath+"\","+"\""+ \
                str(algoPara)+"\",\""+datasetName+"\""+")"
        print sql
        database.insert(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    else:
        sql = "update model set status=1,algoPara="+"\""+str(algoPara)+"\"" +\
                ",datasetName="+"\""+datasetName+"\""+ " where userId="+"\""+ \
                userId+"\" and modelName="+"\""+modelName+"\""        
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    
    #run spark app
    t0 = time.time()
    subCmd = cmdgenerator(algoName,algoPara)
    cmd = "spark-submit "+"--master "+spark_master+" "+cwd+"/spark/python/model.py" + \
                                        " --dataPath=" + dataPath + \
                                        " --modelPath=" + modelPath + \
                                        " --algoName=" + algoName + \
                                        " --dataType=" + dataType
                                        
    cmd += subCmd
    
    status,output = commands.getstatusoutput(cmd)
    print output
    t1 = time.time()
    t = t1 - t0
    
    info = {"status":-1}
    if status == 0:
        #model is trained successfully,update database,set status to 0(finish)
        sql = "update model set status=0,time=" +str(t)+",createTime="+ str(t0)+\
                   " where userId="+"\""+userId+"\" and modelName="+"\""+modelName+"\""
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
        info = {"status":0,"modelName":modelName,"time":t}
    else:
        #failed,set status=-1
        sql = "update model set status=-1,time=" +str(t)+",createTime="+ str(t0)+\
                   " where userId="+"\""+userId+"\" and modelName="+"\""+modelName+"\""
        database.update(mysql_host,mysql_user,mysql_password,"datastudio",sql)
        info["info"] = "train failed"
    
    
    return jsonify(info)

@app.route("/model/prediction",methods=["post"])
def batchPredction():
    userName = request.json["userName"]
    password = request.json["password"]
    datasetName = request.json["datasetName"]
    modelName = request.json["modelName"]
    outputName = request.json["outputName"]
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)
    
    #outputName
    outputPath = searchDataset(userId,outputName)
    if outputPath != "":
        return jsonify({"status":-1,"info":"dataset: "+outputName+" already exists"})
    outputPath = hdfs_path+"/datastudio/user/" + userName + "/dataset/" + outputName
    
    #search dataPath on hdfs
    dataPath = searchDataset(userId,datasetName)
    if dataPath == "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" does not exist"})
    
    #search dataType
    sql = "select dataType from dataset where userId="+userId+" and datasetName="+"\""+datasetName+"\""
    dataType = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)[0][0]
    
    #search modelPath
    modelPath = searchModel(userId,modelName)
    if modelPath == "":
        return jsonify({"status":-1,"info":"model: "+modelName+" does not exist"})
    
    #search algoName
    sql = "select algoName from model where userId="+userId+" and modelName="+"\""+modelName+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)  
    algoName = result[0][0]
    
    #submit spark job
    cmd = "spark-submit "+"--master "+spark_master+" "+cwd+"/spark/python/prediction.py" + \
                                    " --dataPath=" + dataPath + \
                                    " --modelPath=" + modelPath + \
                                    " --dataType=" + dataType + \
                                    " --outputPath=" + outputPath + \
                                    " --algoName=" + algoName
    
    t0 = time.time()
    status,output = commands.getstatusoutput(cmd)   
    t1 = time.time()
    print output
    if status == 0:
        #success
        sql = "insert into dataset(userId,datasetName,dataType,dataPath,createTime) values(" +\
                userId+",\""+outputName+"\","+"\""+"csv"+"\",\""+dataPath+"\","+str(t0)+ ")"
        database.insert(mysql_host,mysql_user,mysql_password,"datastudio",sql)  
        return jsonify({"status":0,"prediction time":t1-t0})
    
    return jsonify({"status":-1,"info":"prediction failed"})                     

def searchDataset(userId,dataset):
    
    sql = "select dataPath from dataset where userId="+ \
                    userId+" and datasetName="+"\""+dataset+"\""
    
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    
    if len(result) == 0:
        return ""
        
    dataPath = result[0][0]
    return dataPath

def searchModel(userId,modelName):
    sql = "select modelPath from model where userId="+ \
                    userId+" and modelName="+"\""+modelName+"\""+ \
                    " and status="+"0"
    
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
    if len(result) == 0:
        return ""
    
    modelPath = result[0][0]
    return modelPath
    
def cmdgenerator(algoName,algoPara):
    """
    generate spark-submit command for each algorithm
    """
    if algoPara == None:
        return None
    cmd = ""    
    
        
    for key in algoPara:
        cmd += " --" + key + "=" + str(algoPara[key])
    
    return cmd
    

def authentication(user,passwd):
    sql = "select userId,password from user where userName="+"\""+user+"\""
    result = database.search(mysql_host,mysql_user,mysql_password,"datastudio",sql)
 
    if len(result) == 0:
        return -1
        
    password = result[0][1]
    userId = result[0][0]
    
    if passwd == password:
        return userId
    else:
        return -1

#upload dataset
@app.route('/dataset/upload', methods=['POST'])
def upload_dataset():
    start_time = time.time()
    #请求参数验证
    if (not request.json or not 'datasetName' in request.json 
                or not 'dataPath' in request.json 
                or not 'dataType' in request.json 
                or not 'userName' in request.json 
                or not 'password' in request.json ):
        return jsonify({'status':-1,"info":"Wrong request para.",'time':-1}),200
    
    userName = request.json['userName']
    password = request.json['password']
    dataType = request.json['dataType']
    datasetName = request.json['datasetName']
    dataPath = request.json['dataPath']
    hdfs_dataPath = hdfs_path + "/datastudio/user/"+userName+"/dataset/"+datasetName+"/"
    local_dataPath = '/home/hadoop/wy/DataStudio/data/'+userName+"/"+dataPath
    
    #Authentication
    userId = authentication(userName,password)
    if userId == -1:
        return jsonify({"status":-1,"info":"authentication failed"})
    userId = str(userId)    
    
    #localfile exist?
    local_file_exists = 'test -e ' + local_dataPath
    local_flag_not_exist = subprocess.call(local_file_exists, shell=True)
    if local_flag_not_exist==1:
        return jsonify({'status':-1,"info":dataPath+" doesn't exist.","time":-1}),200

    #dataset exist?
    dataPath = searchDataset(userId,datasetName)
    if dataPath != "":
        return jsonify({"status":-1,"info":"dataset: "+datasetName+" already exists"})
    
    #create hdfs dir
    cmd = "hadoop fs -mkdir " + hdfs_dataPath
    subprocess.call(cmd, shell=True)    
    
    #upload to hdfs
    shell_to_hdfs = "hadoop fs -put " + local_dataPath + " " + hdfs_dataPath
    subprocess.call(shell_to_hdfs, shell=True)
    createTime = strftime("%Y%m%d%H%M%S")
    
    #update db
    conn = MySQLdb.connect(host=mysql_host, user=mysql_user,
                           passwd=mysql_password, db='datastudio')
    cur = conn.cursor()
    sql_insert = "insert into dataset values('" + userId + "','" + datasetName + "','" + dataType + "','" + hdfs_dataPath + "','" +createTime +"')"
    cur.execute(sql_insert)
    cur.close()
    conn.commit()
    conn.close()
    upload_time = time.time() - start_time
    return jsonify({'status':0, "datasetName":datasetName, "time":upload_time}),200

def loadConfig(path):
    f = open(path)
    config = json.load(f)
    f.close()
    return config["mysql_host"],config["mysql_user"],config["mysql_password"],config["hdfs_path"],config["spark_master"]       

if  __name__ == "__main__":
    mysql_host,mysql_user,mysql_password,hdfs_path,spark_master = loadConfig("config.json")
    cwd = os.getcwd()
    app.run(host="0.0.0.0")
