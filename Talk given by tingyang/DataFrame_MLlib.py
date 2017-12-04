from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
import numpy as np

# read data as DataFrame and header as list
data = spark.read.csv("file:///scratch/tix11001/spark/taxiData/trip_taxi",header = True, inferSchema = True)
data.show()
data.printSchema()

# Analysis the statistics
stat = data.describe()
stat.show()
#stat.rdd.saveAsTextFile("file:///scratch/BigDataProject_gul15104_ajp06003/NYCtaxi/03SparkData/data/stat")

# Filter the data
data_cleaned = data.filter((data['passenger_count'] > 0) & (data['passenger_count'] < 10) & (data['trip_time_in_secs'] > 0) & (data['trip_time_in_secs'] < 3000) & (data['trip_distance'] >0) & (data['trip_distance'] < 25) & (data['fare_amount'] > 0) & (data['fare_amount'] < 50) & (data['total_amount'] > 0) & (data['total_amount'] < 100) & (data['tip_amount'] >0) & (data['tip_amount'] < 20) & (data['pickup_longitude'] > -75) & (data['pickup_longitude'] < -73) & (data['pickup_latitude'] > 30) & (data['pickup_latitude'] < 43))

# Alternative way of filtering data
#data.registerTempTable("taxi")
#df_sql = sqlContext.sql("SELECT * FROM taxi WHERE passenger_count >0 and passenger_count < 10 and trip_time_in_secs > 0 and trip_time_in_secs < 3000 and trip_distance > 0 AND trip_distance < 25 AND fare_amount > 0 AND fare_amount < 50 AND total_amount>0 AND total_amount < 100 and tip_amount > 0 and tip_amount < 20")
#df_sql.show() # Show my result
# df_sql.registerTempTable("taxi_clean")
# sqlContext.sql("SELECT count(*) FROM taxi_clean").show()# calculate the number of rows 

############## regression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

# Merge all needed columns as one column called features 
assembler = VectorAssembler(inputCols = ['trip_time_in_secs', 'trip_distance'], outputCol="features")
# Select features and rename total_amount as label.
 regression_data = assembler.transform(data_cleaned).select([col for col in data_cleaned.columns if col != "total_amount"]+["features",data_cleaned["total_amount"].alias("label")] )
regression_data.show()

# Setup the linear regression solver
lr = LinearRegression(maxIter=1000, regParam=0.3, elasticNetParam=0)
# Fit the model
lrModel = lr.fit(regression_data)
# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# Test the model
test_data = regression_data
predictions = lrModel.transform(test_data)
predictions.select("prediction","label","features").show()

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


# Save and load model
lrModel.save("file:///scratch/tix11001/spark/regression1")
linearModel1 = LinearRegressionModel.load("file:///scratch/tix11001/spark/regression1")

# K-means
from pyspark.ml.clustering import KMeans

assembler = VectorAssembler(inputCols = ["pickup_longitude", "pickup_latitude"], outputCol="features")
data_kmeans = assembler.transform(data_kmeans)

# fit the K-means model
kmeans = KMeans().setK(7).setSeed(1)
kmeansModel = kmeans.fit(data_kmeans)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = kmeansModel.computeCost(data_kmeans)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
centers = kmeansModel.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
	
# Predict clusters
kmeans_predictions = kmeansModel.transform(data_kmeans)
kmeans_predictions.select("prediction").show() 
kmeans_predictions.filter(kmeans_predictions["prediction"] == 0).count()
kmeans_predictions.filter(kmeans_predictions["prediction"] == 1).count()




















# Linear Regression via RDD
regression_data = sqlContext.sql("SELECT total_amount, trip_time_in_secs, trip_distance FROM taxi_clean").rdd.map(list)

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

# Load and parse the data
def parsePoint(line):
    return LabeledPoint(line[0], line[1:])

#data = sc.textFile("data/mllib/ridge-data/lpsa.data")
parsedData = regression_data.map(parsePoint)
# parsedData = regression_data.map(lambda line: LabeledPoint(line[0], line[1:]))

# Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=100000, step=0.001)
model.weights
model.intercept


# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))

def squares( line ):
    return (line[0]-line[1])**2

MSE = valuesAndPreds.map(squares).reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

