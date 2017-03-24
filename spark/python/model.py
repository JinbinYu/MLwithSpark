from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
import sys
import argparse
import json

def logisticRegression(df,arguments):
	"""
	Only supports binary classification
	"""
	from pyspark.ml.classification import LogisticRegression
	maxIter = 100
	regParam = 0
	elasticNetParam = 0

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.regParam != None:
		regParam = float(arguments.regParam)

	if arguments.elasticNetParam != None:
		elasticNetParam = float(arguments.elasticNetParam)

	lr = LogisticRegression(maxIter=maxIter,
							regParam=regParam,
							elasticNetParam=elasticNetParam)
	lrModel = lr.fit(df)

	return lrModel

def linearRegression(df,arguments):
	from pyspark.ml.regression import LinearRegression
	maxIter = 100
	regParam = 0
	elasticNetParam = 0

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.regParam != None:
		regParam = float(arguments.regParam)

	if arguments.elasticNetParam != None:
		elasticNetParam = float(arguments.elasticNetParam)

	lr = LinearRegression(maxIter=maxIter,
						  regParam=regParam,
						  elasticNetParam=elasticNetParam)
	lrModel = lr.fit(df)

	return lrModel

def kMeans(df,arguments):
	from pyspark.ml.clustering import KMeans
	k = 2
	maxIter = 20

	if arguments.maxIter != None:
		maxIter = float(arguments.maxIter)

	if arguments.k != None:
		k = float(arguments.k)

	km = KMeans(k=k,maxIter=maxIter)
	model = km.fit(df)

	return model

def decisionTreeClassification(df,arguments):
	from pyspark.ml.classification import DecisionTreeClassifier
	maxDepth = 5
	minInstancesPerNode = 1
	impurity = "gini"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.impurity != None:
		impurity = arguments.impurity

	dt = DecisionTreeClassifier(maxDepth=maxDepth,
								minInstancesPerNode=minInstancesPerNode,
								impurity=impurity)
	model = dt.fit(df)

	return model

def decisionTreeRegression(df,arguments):
	from pyspark.ml.regression import DecisionTreeRegressor
	maxDepth = 5
	minInstancesPerNode = 1
	impurity = "variance"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.impurity != None:
		impurity = arguments.impurity

	dt = DecisionTreeRegressor(maxDepth=maxDepth,
							   minInstancesPerNode=minInstancesPerNode,
							   impurity=impurity)
	model = dt.fit(df)

	return model

def randomForestClassification(df,arguments):
	from pyspark.ml.classification import RandomForestClassifier
	maxDepth = 5
	minInstancesPerNode = 1
	numTrees = 20
	impurity = "gini"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.impurity != None:
		impurity = arguments.impurity

	rf =  RandomForestClassifier(numTrees=numTrees,
								 maxDepth=maxDepth,
								 minInstancesPerNode=minInstancesPerNode,
								 impurity=impurity)
	model = rf.fit(df)

	return model

def randomForestRegression(df,arguments):
	from pyspark.ml.regression import RandomForestRegressor
	maxDepth = 5
	minInstancesPerNode = 1
	numTrees = 20
	impurity = "variance"

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.impurity != None:
		impurity = arguments.impurity

	rf =  RandomForestRegressor(numTrees=numTrees,
								maxDepth=maxDepth,
								minInstancesPerNode=minInstancesPerNode,
								impurity=impurity)
	model = rf.fit(df)

	return model

def gbdtClassification(df,arguments):
	from pyspark.ml.classification import GBTClassifier
	numTrees = 20
	stepSize = 0.1
	maxDepth = 5
	minInstancesPerNode = 1

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.stepSize != None:
		stepSize = float(arguments.stepSize)

	gbdt = GBTClassifier(maxIter=numTrees,
						 stepSize=stepSize,
						 maxDepth=maxDepth,
						 minInstancesPerNode=minInstancesPerNode)
	model = gbdt.fit(df)

	return model

def gbdtRegression(df,arguments):
	from pyspark.ml.regression import GBTRegressor
	numTrees = 20
	stepSize = 0.1
	maxDepth = 5
	minInstancesPerNode = 1

	if arguments.maxDepth != None:
		maxDepth = float(arguments.maxDepth)

	if arguments.minInstancesPerNode != None:
		minInstancesPerNode = float(arguments.minInstancesPerNode)

	if arguments.numTrees != None:
		numTrees = float(arguments.numTrees)

	if arguments.stepSize != None:
		stepSize = float(arguments.stepSize)

	if arguments.impurity != None:
		impurity = arguments.impurity

	gbdt = GBTRegressor(maxIter=numTrees,
						stepSize=stepSize,
						maxDepth=maxDepth,
						minInstancesPerNode=minInstancesPerNode)
	model = gbdt.fit(df)

	return model

if __name__ == "__main__":

	#set argparser
	parser = argparse.ArgumentParser()
	
	#basic parameters	
	#parser.add_argument("--datasetName")
	parser.add_argument("--dataType")
	parser.add_argument("--algoName")
	parser.add_argument("--dataPath")
	parser.add_argument("--modelPath")
	#parser.add_argument("--userName")
	#algorithm parameters
	parser.add_argument("--maxIter")	#for LogistcRegression,LinearRegression,KMeans
	parser.add_argument("--regParam")	#for LogistcRegression,LinearRegression
	parser.add_argument("--elasticNetParam")	#for LogistcRegression,LinearRegression
	parser.add_argument("--k")			#for KMeans
	parser.add_argument("--numTrees")	#for RandomForest,GBDT
	parser.add_argument("--minInstancesPerNode")	#for RandomForest,GBDT
	parser.add_argument("--maxDepth")	#for DecisionTree,RandomForest,GBDT
	parser.add_argument("--impurity")	#for DecisionTree,RandomForest
	parser.add_argument("--stepSize")	#for GBDT

	#parse args
	arguments = parser.parse_args()

	#initial spark environment
	appName = "model training"
	conf = SparkConf().setAppName(appName).setMaster("spark://ubuntu:7077")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	#get basic parameters
	dataType = arguments.dataType
	algoName = arguments.algoName


	#read data
	#support dataType:libsvm,csv,hive
	df = None
	dataPath = arguments.dataPath

	if dataType == "libsvm":
		df = sqlContext.read.format("libsvm").load(dataPath)


	#train model
	model = None

	if algoName == "LogisticRegression":
		model = logisticRegression(df,arguments)
	elif algoName == "LinearRegression":
		model = linearRegression(df,arguments)
	elif algoName == "KMeans":
		model = kMeans(df,arguments)
	elif algoName == "DecisionTreeClassification":
		model = decisionTreeClassification(df,arguments)
	elif algoName == "DecisionTreeRegression":
		model = decisionTreeRegression(df,arguments)
	elif algoName == "RandomForestClassification":
		model = randomForestClassification(df,arguments)
	elif algoName == "RandomForestRegression":
		model = randomForestRegression(df,arguments)
	elif algoName == "GBTClassification":
		model = gbdtClassification(df,arguments)
	elif algoName == "GBTRegression":
		model = gbdtRegression(df,arguments)


	#save model(overwrite if exists)
	modelPath = arguments.modelPath
	model.write().overwrite().save(modelPath)