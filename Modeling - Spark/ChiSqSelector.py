import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.feature import ChiSqSelector

# Creates Spark Context
sc = SparkContext()
sql_sc = SQLContext(sc)

# Uses panda to read csv file
pandas_df = pd.read_csv('data.csv', na_values=[' '])
spark_df = sql_sc.createDataFrame(pandas_df)

# Gets all column names as list
df_columns = spark_df.columns

# Removes ID, categorical(Year, Zip) and label features
df_columns = df_columns[3:-1]

# Step 1 - VectorAssembler
assembler = VectorAssembler(inputCols = df_columns, outputCol = "features")
output = assembler.transform(spark_df)
output = output.select("features", "HousingMedianRent$")

# Step 2 - ChiSqSelector
selector = ChiSqSelector(numTopFeatures = 50, featuresCol = "features", outputCol = "selectedFeatures", labelCol = "HousingMedianRent$")
result = selector.fit(output).transform(output)
model = selector.fit(output)
importantFeatures = model.selectedFeatures
# Shows all important features selected by ChiSqSelector
print("ChiSqSelector with top %d features selected:" % selector.getNumTopFeatures())
for i in range(len(importantFeatures)):
	print df_columns[importantFeatures[i]]

#result.show()

sc.stop()
