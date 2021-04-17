# Databricks notebook source
import matplotlib.pyplot as plt
%matplotlib inline

fig, axs = plt.subplots(3, 3, figsize=(9, 9), sharey=True)
  
for i in range (1,3): 
  data = {1: 10+i, 2: 15+i, 3: 5+i, 4: 20+i, 5: 22+i}
  names = list(data.keys())
  values = list(data.values())

  axs[0,i].bar(names, values)
  axs[1,i].scatter(names, values)
  axs[2,i].plot(names, values)
  fig.suptitle('Categorical Plotting')

  display()

# COMMAND ----------

fig, axs = plt.subplots(1, 3, figsize=(9, 3), sharey=True)
axs[0].bar(names, values)
axs[1].scatter(names, values)
axs[2].plot(names, values)
fig.suptitle('Categorical Plotting')

display()

# COMMAND ----------


cat = ["bored", "happy", "bored", "bored", "happy", "bored"]
dog = ["happy", "happy", "happy", "happy", "bored", "bored"]
activity = ["combing", "drinking", "feeding", "napping", "playing", "washing"]

fig, ax = plt.subplots()
ax.plot(activity, dog, label="dog")
ax.plot(activity, cat, label="cat")
ax.legend()

plt.show()

# COMMAND ----------

from pyspark import SparkContext 
from pyspark.sql import SparkSession 

sc = SparkContext.getOrCreate()

if (sc is None):
   sc = SparkContext(master="local[]", appName="Test")
    
spark = SparkSession(sparkContext=sc)

df = spark.createDataFrame([
   ("2.PA1234.l", "1234","10","Ann"),
   ("10.PA125.la", None,"20", "Tuta"),
   ("2.PANA.ln", None,"30",None),
   (None, "156",None,"Tom"),
   (None, "156",None,"Ann"),
   (None, "156","35","Ann"),
   ("2.PANA.ln", "200",None,"Karen")], 
    ["address", "st", "Age","Name"])

# COMMAND ----------

df.show()

# COMMAND ----------

import pyspark.sql.functions as f

def compute_mode(df, col):
  cnts = df.select([col]).na.drop().groupBy(col).count()
  mode = cnts.join(cnts.agg(f.max("count").alias("max_")), f.col("count") == f.col("max_")).limit(1).select(col)
  mode_val = mode.first()[0]
  return(mode_val)

for col in df.columns:
  df = df.na.fill({col: compute_mode(df, col)})

df.show() 

# COMMAND ----------

 

# COMMAND ----------

df.columns

# COMMAND ----------

#df.na.drop().show()
df.select(["st","Age"]).na.drop().show()
#df.na.drop(["st","Age"]).show()

# COMMAND ----------

#df.na.drop(["onlyColumnInOneColumnDataFrame"])
df.select("Age").na.drop().show()
#df.na.drop(subset=["Age"]).show()

# COMMAND ----------

from pyspark import SparkContext 
from pyspark.sql import SparkSession 

sc = SparkContext.getOrCreate()

if (sc is None):
   sc = SparkContext(master="local[]", appName="Test")
    
spark = SparkSession(sparkContext=sc)

df = spark.createDataFrame([
   ("2.PA1234.l", "1234","10","Ann"),
   ("10.PA125.la", None,"20", "Tuta"),
   ("2.PANA.ln", None,"30","NA"),
   (None, "156",None,"Tom"),
   (None, "156",None,"Ann"),
   (None, "156","35","Ann"),
   ("2.PANA.ln", "200",None,"Karen")], 
    ["address", "st", "Age","Name"])

# COMMAND ----------

import pyspark.sql.functions as f
df_temp = df.groupBy('Name').agg((f.count(f.lit(1))/df.count()).alias("frequency"))
most_frequent_name = df_temp.sort(f.col('frequency').desc()).select('Name').first()[0]
df_temp = df_temp.withColumn('Name_replaced',
                             f.when(f.col("Name")=='NA', f.lit(most_frequent_name)).otherwise(f.col('Name')))

df_final = df.join(df_temp, df.Name==df_temp.Name, 'left').drop(df_temp.Name).drop("frequency")
df_final.show()

# COMMAND ----------

