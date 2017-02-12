import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.regression import LinearRegression, LinearRegressionTrainingSummary
from pyspark.ml.evaluation import RegressionEvaluator

sc = SparkContext()
sql_sc = SQLContext(sc)
pandas_df = pd.read_csv('data.csv', na_values=[' '])
spark_df = sql_sc.createDataFrame(pandas_df)

df_columns = spark_df.columns
df_columns = df_columns[3:-1]
assembler = VectorAssembler(inputCols = df_columns, outputCol = "features")
vector_df = assembler.transform(spark_df)
data = vector_df.select("features", "HousingMedianRent$")
(trainingData, testData) = data.randomSplit([0.7, 0.3])

lr = LinearRegression(labelCol = "HousingMedianRent$")
model = lr.fit(trainingData, {lr.regParam:0.3})
predictions = model.transform(testData)
predictions.select("features", "HousingMedianRent$", "prediction").show(5)

#summary = model.summary
#print("Linear Regression result:")
#print("Root Mean Squared Error (RMSE): " + str(summary.rootMeanSquaredError))
#print("Mean Squared Error (MSE): " + str(summary.meanSquaredError))
#print("Mean Absolute Error (MAE): " + str(summary.meanAbsoluteError))
#print("R-squared: " + str(summary.r2))
#print("Intercept: " + str(model.intercept))
#print("Coefficients: " + str(model.coefficients))
#print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))

evaluator_rmse = RegressionEvaluator(labelCol="HousingMedianRent$", predictionCol="prediction", metricName="rmse")
evaluator_mse = RegressionEvaluator(labelCol="HousingMedianRent$", predictionCol="prediction", metricName="mse")
evaluator_r2 = RegressionEvaluator(labelCol="HousingMedianRent$", predictionCol="prediction", metricName="r2")
evaluator_mae = RegressionEvaluator(labelCol="HousingMedianRent$", predictionCol="prediction", metricName="mae")

rmse = evaluator_rmse.evaluate(predictions)
mse = evaluator_mse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)

print("Linear Regression result:")
print("Root Mean Squared Error (RMSE): %g" % rmse)
print("Mean Squared Error (MSE): %g" % mse)
print("Mean Absolute Error (MAE): %g" % mae)
print("R-squared: %g" % r2)

sc.stop()
