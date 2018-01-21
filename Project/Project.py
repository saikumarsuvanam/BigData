# Databricks notebook source
#Matthew Chen mdc150630
#Somayeh Mohammadpour sxm155431
#Suvanam Saikumar sxs155933

# COMMAND ----------

#File uploaded to /FileStore/tables/dengue_labels_train.csv
#File uploaded to /FileStore/tables/dengue_features_train.csv
#File uploaded to /FileStore/tables/dengue_features_test.csv

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, round
import os
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.ml.feature import StringIndexer, VectorAssembler, PCA
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import GeneralizedLinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

labelsFile = "/FileStore/tables/dengue_labels_train.csv";
trainFile = "/FileStore/tables/dengue_features_train.csv";
testFile = "/FileStore/tables/dengue_features_test.csv";

# COMMAND ----------

labels = spark.read.option("header","true"). option("inferSchema","true").csv(labelsFile)
train = spark.read.option("header","true"). option("inferSchema","true").csv(trainFile)
test = spark.read.option("header","true"). option("inferSchema","true").csv(testFile)

# COMMAND ----------

labelsDif = labels.select(col("city").alias("c"), col("year").alias("y"), col("weekofyear").alias("woy"), col("total_cases"))

# COMMAND ----------

trainData = train.join(labelsDif, (train.city == labelsDif.c) & (train.year == labelsDif.y) & (train.weekofyear == labelsDif.woy))
trainData = trainData.select(col("city"), col("year"), col("weekofyear"), col("week_start_date").cast("long"), col("ndvi_ne"), col("ndvi_nw"), col("ndvi_se"), col("ndvi_sw"), col("precipitation_amt_mm"), col("reanalysis_air_temp_k"), col("reanalysis_avg_temp_k"), col("reanalysis_dew_point_temp_k"), col("reanalysis_max_air_temp_k"), col("reanalysis_min_air_temp_k"), col("reanalysis_precip_amt_kg_per_m2"), col("reanalysis_relative_humidity_percent"), col("reanalysis_sat_precip_amt_mm"), col("reanalysis_specific_humidity_g_per_kg"), col("reanalysis_tdtr_k"), col("station_avg_temp_c"), col("station_diur_temp_rng_c"), col("station_max_temp_c"), col("station_min_temp_c"), col("station_precip_mm"), col("total_cases").alias("label"))

# COMMAND ----------

testData = test.select(col("city"), col("year"), col("weekofyear"), col("week_start_date").cast("long"), col("ndvi_ne"), col("ndvi_nw"), col("ndvi_se"), col("ndvi_sw"), col("precipitation_amt_mm"), col("reanalysis_air_temp_k"), col("reanalysis_avg_temp_k"), col("reanalysis_dew_point_temp_k"), col("reanalysis_max_air_temp_k"), col("reanalysis_min_air_temp_k"), col("reanalysis_precip_amt_kg_per_m2"), col("reanalysis_relative_humidity_percent"), col("reanalysis_sat_precip_amt_mm"), col("reanalysis_specific_humidity_g_per_kg"), col("reanalysis_tdtr_k"), col("station_avg_temp_c"), col("station_diur_temp_rng_c"), col("station_max_temp_c"), col("station_min_temp_c"), col("station_precip_mm"))

# COMMAND ----------

#trainData = trainData.dropna()
#testData = testData.dropna()

# COMMAND ----------

trainData = trainData.na.fill(0)
testData = testData.na.fill(0)

# COMMAND ----------

#21 features
assembler = VectorAssembler(inputCols=["week_start_date", "ndvi_ne", "ndvi_nw", "ndvi_se", "ndvi_sw", "precipitation_amt_mm", "reanalysis_air_temp_k", "reanalysis_avg_temp_k", "reanalysis_dew_point_temp_k", "reanalysis_max_air_temp_k", "reanalysis_min_air_temp_k", "reanalysis_precip_amt_kg_per_m2", "reanalysis_relative_humidity_percent", "reanalysis_sat_precip_amt_mm", "reanalysis_specific_humidity_g_per_kg", "reanalysis_tdtr_k", "station_avg_temp_c", "station_diur_temp_rng_c", "station_max_temp_c", "station_min_temp_c", "station_precip_mm"], outputCol="features")

# COMMAND ----------

trainingData = assembler.transform(trainData)
testingData = assembler.transform(testData)

# COMMAND ----------

pca = PCA(k=14, inputCol="features", outputCol="pcaFeatures")
pipelineFilt = Pipeline(stages=[pca])

# COMMAND ----------

trainingData2 = pipelineFilt.fit(trainingData).transform(trainingData)
testingData2 = pipelineFilt.fit(testingData).transform(testingData)

# COMMAND ----------

trainingData2 = trainingData2.select(col("city"), col("year"), col("weekofyear"), col("week_start_date").cast("long"), col("ndvi_ne"), col("ndvi_nw"), col("ndvi_se"), col("ndvi_sw"), col("precipitation_amt_mm"), col("reanalysis_air_temp_k"), col("reanalysis_avg_temp_k"), col("reanalysis_dew_point_temp_k"), col("reanalysis_max_air_temp_k"), col("reanalysis_min_air_temp_k"), col("reanalysis_precip_amt_kg_per_m2"), col("reanalysis_relative_humidity_percent"), col("reanalysis_sat_precip_amt_mm"), col("reanalysis_specific_humidity_g_per_kg"), col("reanalysis_tdtr_k"), col("station_avg_temp_c"), col("station_diur_temp_rng_c"), col("station_max_temp_c"), col("station_min_temp_c"), col("station_precip_mm"), col("label"), col("pcaFeatures").alias("features"))
testingData2 = testingData2.select(col("city"), col("year"), col("weekofyear"), col("week_start_date").cast("long"), col("ndvi_ne"), col("ndvi_nw"), col("ndvi_se"), col("ndvi_sw"), col("precipitation_amt_mm"), col("reanalysis_air_temp_k"), col("reanalysis_avg_temp_k"), col("reanalysis_dew_point_temp_k"), col("reanalysis_max_air_temp_k"), col("reanalysis_min_air_temp_k"), col("reanalysis_precip_amt_kg_per_m2"), col("reanalysis_relative_humidity_percent"), col("reanalysis_sat_precip_amt_mm"), col("reanalysis_specific_humidity_g_per_kg"), col("reanalysis_tdtr_k"), col("station_avg_temp_c"), col("station_diur_temp_rng_c"), col("station_max_temp_c"), col("station_min_temp_c"), col("station_precip_mm"), col("pcaFeatures").alias("features"))

# COMMAND ----------

display(trainingData2)

# COMMAND ----------

display(testingData2)

# COMMAND ----------

#START HERE

# COMMAND ----------

#Random Forests Documentation/Example
#https://spark.apache.org/docs/2.2.0/mllib-ensembles.html#random-forests

# COMMAND ----------

#Generalized Linear Regression Documentation/Example
#https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#generalized-linear-regression
glm = GeneralizedLinearRegression(family="poisson", link="sqrt")
rfr = RandomForestRegressor(impurity="variance", numTrees=50)

# COMMAND ----------

#trainingDataDF, testingDataDF = trainingData2.randomSplit([0.8, 0.2], seed=0L)

# COMMAND ----------

pipeline = Pipeline(stages=[glm])
pipeline2 = Pipeline(stages=[rfr])

# COMMAND ----------

paramGrid = ParamGridBuilder().addGrid(glm.maxIter, [8, 10, 12]).addGrid(glm.regParam, [0.4, 0.6, 0.8]).build()
paramGrid2 = ParamGridBuilder().addGrid(rfr.maxDepth, [20, 25]).addGrid(rfr.maxBins, [32, 48]).build()

# COMMAND ----------

crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(metricName = "mae"), numFolds=5)
crossval2 = CrossValidator(estimator=pipeline2, estimatorParamMaps=paramGrid2, evaluator=RegressionEvaluator(metricName = "mae"), numFolds=5)

# COMMAND ----------

trainingDataSJ = trainingData2.filter("city == 'sj'")
trainingDataIQ = trainingData2.filter("city == 'iq'")
testingDataSJ = testingData2.filter("city == 'sj'")
testingDataIQ = testingData2.filter("city == 'iq'")
#testingData2SJ = testingData2.filter("city == 'sj'")
#testingData2IQ = testingData2.filter("city == 'iq'")

# COMMAND ----------

cvModel = crossval2.fit(trainingDataSJ)#RFR
cvModel2 = crossval2.fit(trainingDataIQ)#RFR
cvModel3 = crossval.fit(trainingDataSJ)#GLM
cvModel4 = crossval.fit(trainingDataIQ)#GLM

# COMMAND ----------

cvModelF = cvModel.transform(trainingDataSJ)#RFR
cvModel2F = cvModel2.transform(trainingDataIQ)#RFR
cvModel3F = cvModel3.transform(trainingDataSJ)#GLM
cvModel4F = cvModel4.transform(trainingDataIQ)#GLM

# COMMAND ----------

predictionsAndLabels = cvModelF.select(col("city"), col("year"), col("weekofyear"), col("label").cast("double"), round(col("prediction")))
predictionsAndLabels2 = cvModel2F.select(col("city"), col("year"), col("weekofyear"), col("label").cast("double"), round(col("prediction")))
predictionsAndLabels3 = cvModel3F.select(col("city"), col("year"), col("weekofyear"), col("label").cast("double"), round(col("prediction")))
predictionsAndLabels4 = cvModel4F.select(col("city"), col("year"), col("weekofyear"), col("label").cast("double"), round(col("prediction")))

# COMMAND ----------

display(predictionsAndLabels)

# COMMAND ----------

display(predictionsAndLabels2)

# COMMAND ----------

display(predictionsAndLabels3)

# COMMAND ----------

display(predictionsAndLabels4)

# COMMAND ----------

PAL = predictionsAndLabels.rdd.map(lambda row: (row[0].encode('ascii', 'ignore'), row[1], row[2], row[3], row[4])).map(lambda x: (x[4], x[3]))
PAL2 = predictionsAndLabels2.rdd.map(lambda row: (row[0].encode('ascii', 'ignore'), row[1], row[2], row[3], row[4])).map(lambda x: (x[4], x[3]))
PAL3 = predictionsAndLabels3.rdd.map(lambda row: (row[0].encode('ascii', 'ignore'), row[1], row[2], row[3], row[4])).map(lambda x: (x[4], x[3]))
PAL4 = predictionsAndLabels4.rdd.map(lambda row: (row[0].encode('ascii', 'ignore'), row[1], row[2], row[3], row[4])).map(lambda x: (x[4], x[3]))

# COMMAND ----------

metrics = RegressionMetrics(PAL)
metrics2 = RegressionMetrics(PAL2)
metrics3 = RegressionMetrics(PAL3)
metrics4 = RegressionMetrics(PAL4)

# COMMAND ----------

print(metrics.meanAbsoluteError)
print(metrics2.meanAbsoluteError)
print(metrics3.meanAbsoluteError)
print(metrics4.meanAbsoluteError)

# COMMAND ----------

predictingTestRFRSJ = cvModel.transform(testingDataSJ)
predictingTestRFRIQ = cvModel2.transform(testingDataIQ)
predictingTestRFRSJ2 = cvModel3.transform(testingDataSJ)
predictingTestRFRIQ2 = cvModel4.transform(testingDataIQ)

# COMMAND ----------

predictionsAndLabelsF = predictingTestRFRSJ.select(col("city"), col("year"), col("weekofyear"), round(col("prediction")).alias("total_cases"))
predictionsAndLabels2F = predictingTestRFRIQ.select(col("city"), col("year"), col("weekofyear"), round(col("prediction")).alias("total_cases"))
predictionsAndLabels3F = predictingTestRFRSJ2.select(col("city"), col("year"), col("weekofyear"), round(col("prediction")).alias("total_cases"))
predictionsAndLabels4F = predictingTestRFRIQ2.select(col("city"), col("year"), col("weekofyear"), round(col("prediction")).alias("total_cases"))

# COMMAND ----------

combineResults = predictionsAndLabelsF.union(predictionsAndLabels2F)
combineResults2 = predictionsAndLabels3F.union(predictionsAndLabels4F)

# COMMAND ----------

display(combineResults)

# COMMAND ----------

display(combineResults2)
