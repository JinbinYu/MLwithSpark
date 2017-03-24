from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
import argparse

if __name__ == "__main__":
	#set argparser
	parser = argparse.ArgumentParser()
	parser.add_argument("--algoName")
	parser.add_argument("--dataPath")
	parser.add_argument("--outputPath")
	parser.add_argument("--modelPath")
	parser.add_argument("--dataType")

	arguments = parser.parse_args()
	algoName = arguments.algoName
	dataPath = arguments.dataPath
	modelPath = arguments.modelPath
	outputPath = arguments.outputPath
	dataType = arguments.dataType

	#initial spark environment
	appName = "model batch prediction"
	conf = SparkConf().setAppName(appName).setMaster("spark://ubuntu:7077")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	#load data
	data = None
	if dataType == "libsvm":
		data = sqlContext.read.format("libsvm").load(dataPath)


	#load model
	if algoName == "LogisticRegression":
		from pyspark.ml.classification import LogisticRegressionModel
		model = LogisticRegressionModel.load(modelPath)
	elif algoName == "LinearRegression":
		from pyspark.ml.regression import LinearRegressionModel
		model = LinearRegressionModel.load(modelPath)
	elif algoName == "DecisionTreeClassification":
		from pyspark.ml.classification import DecisionTreeClassificationModel
		model = DecisionTreeClassificationModel.load(modelPath)
	elif algoName == "DecisionTreeRegression":
		from pyspark.ml.regression import DecisionTreeRegressionModel
		model = DecisionTreeRegressionModel.load(modelPath)
	elif algoName == "RandomForestClassification":
		from pyspark.ml.classification import RandomForestClassificationModel
		model = RandomForestClassificationModel.load(modelPath)
	elif algoName == "RandomForestRegression":
		from pyspark.ml.regression import RandomForestRegressionModel
		model = RandomForestRegressionModel.load(modelPath)
	elif algoName == "GBTClassification":
		from pyspark.ml.classification import GBTClassificationModel
		model = GBTClassificationModel.load(modelPath)
	elif algoName == "GBTRegression":
		from pyspark.ml.regression import GBTRegressionModel
		model = GBTRegressionModel.load(modelPath)

	#predict
	prediction = model.transform(data).select("prediction")

	#save
	prediction.write.format("csv").save(outputPath)