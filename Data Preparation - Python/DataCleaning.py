import csv
import numpy as np
import pandas as pd

df = pd.read_csv('../Data/CombinedData.csv', na_values=[' '])
#print 'Number of cols before cleaning:', len(df.columns)
#print 'Number of rows before cleaning:', len(df)

# drop rows that have missing values more than 20% of columns 
# thresh means it requires that many non-NA values in that row/column
df = df.dropna(thresh = 0.85 * len(df.columns))
#df = df.dropna(thresh = 0.85 * len(df), axis = 1)

#print 'Number of cols after cleaning:', len(df.columns)
#print 'Number of rows after cleaning:', len(df)

df.to_csv('../Data/CombinedData_clean_.csv', encoding='utf-8', index=False)