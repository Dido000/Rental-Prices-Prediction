import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoder, StringIndexer

# Creates Spark Context
sc = SparkContext()
sql_sc = SQLContext(sc)

# Uses panda to read csv file
pandas_df = pd.read_csv('data.csv', na_values=[' '])
spark_df = sql_sc.createDataFrame(pandas_df)

# Step 1 - StringIndexer
firstIndexer = StringIndexer(inputCol="Year", outputCol="YearIndex")
firstModel = firstIndexer.fit(spark_df)
firstIndexed = firstModel.transform(spark_df)
secondIndexer = StringIndexer(inputCol="Zip", outputCol="ZipIndex")
secondModel = secondIndexer.fit(firstIndexed)
secondIndexed = secondModel.transform(firstIndexed)

# Step 2 - OneHotEncoder
yearEncoder = OneHotEncoder(dropLast=False, inputCol="YearIndex", outputCol="YearVec")
zipEncoder = OneHotEncoder(dropLast=False, inputCol="ZipIndex", outputCol="ZipVec")
yearEncoded = yearEncoder.transform(secondIndexed)
zipEncoded = zipEncoder.transform(yearEncoded)
final_df = zipEncoded

final_df.select("YearIndex","YearVec","ZipIndex","ZipVec").show()
