# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in AWS S3. There are two ways to establish access to S3: [IAM roles](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) and access keys.
# MAGIC 
# MAGIC *We recommend using IAM roles to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.* If you do use keys, you'll have to escape the `/` in your keys with `%2F`.
# MAGIC 
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "s3n://udacity-dsnd/sparkify/mini_sparkify_event_data.json"
file_type = "json"

# CSV options
infer_schema = "{{infer_schema}}"
first_row_is_header = "{{first_row_is_header}}"
delimiter = "{{delimiter}}"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table
temp_table_name = "mini_sparkify_event_data"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from mini_sparkify_event_data

# COMMAND ----------

# MAGIC %sql
# MAGIC /* number of users */
# MAGIC 
# MAGIC select count(distinct userid) from mini_sparkify_event_persist

# COMMAND ----------

# Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "mini_sparkify_event_persist"
df.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from mini_sparkify_event_persist Limit 5

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print((df.count(), len(df.columns)))

# COMMAND ----------

import databricks.koalas as ks

# COMMAND ----------

kdf = ks.DataFrame(df)

# COMMAND ----------

kdf.shape

# COMMAND ----------

kdf['userId'].nunique()

# COMMAND ----------

kdf.head()

# COMMAND ----------

kdf.info()

# COMMAND ----------

kdf1 = kdf.copy() 
#kdf1['ts_date'] = ks.to_datetime(kdf1['ts'].astype(str),unit='ms')

# COMMAND ----------

kdf1.head()

# COMMAND ----------

kdf1.isna().sum()

# COMMAND ----------

mask = kdf1['registration'].isna()
kdf1[~mask].isna().sum()

# COMMAND ----------

kdf1 = kdf1[~mask]

# COMMAND ----------

kdf1.isna().sum()

# COMMAND ----------

kdf1.shape

# COMMAND ----------

#kdf1['ts'].astype(str)
kdf1['ts_date'] = ks.to_datetime(kdf1['ts'].astype(str),unit='ms')

# COMMAND ----------

kdf1['registration_date'] = ks.to_datetime(kdf1['registration'].astype(str),unit='ms')

# COMMAND ----------

kdf1.info()

# COMMAND ----------

kdf1.head()

# COMMAND ----------

kdf1= kdf1.drop(['ts', 'registration'], axis=1)
kdf1.head()

# COMMAND ----------

kdf1[kdf1['userId'] == 100001]

# COMMAND ----------

kdf1[kdf1['userId'] == 100015]['page'].unique()

# COMMAND ----------

kdf1.shape

# COMMAND ----------

kdf1['level'].value_counts().plot(kind = 'bar')

# COMMAND ----------

mask = kdf1['page'] == 'Cancellation Confirmation'
kdf1[mask]['userId'].nunique()

# COMMAND ----------

kdf1[kdf1['userId'] == 11]['page'].value_counts()

# COMMAND ----------

user_event_count_df = kdf1.groupby(['userId','page']).agg({'page': 'count'}).rename(columns={"page": "page_count"}).reset_index()
user_event_count_df.head()

# COMMAND ----------

user_event_count_df = user_event_count_df.pivot(index="userId", columns="page",values="page_count").fillna(0)
user_event_count_df.head(30)

# COMMAND ----------

user_event_count_df['churn'] = 0
mask = (user_event_count_df['Submit Downgrade'] > 0) | (user_event_count_df['Cancellation Confirmation'] > 0 )
user_event_count_df['churn'] = mask

# COMMAND ----------

user_event_count_df = user_event_count_df.drop(['Submit Downgrade', 'Cancellation Confirmation']).reset_index()
user_event_count_df.head(30)

# COMMAND ----------

user_event_count_df.shape

# COMMAND ----------

kdf1.groupby('userId').agg({'page': 'last'}).head()

# COMMAND ----------

mask = (kdf1['page'] == 'Submit Downgrade') | (kdf1['page'] == 'Cancellation Confirmation')

user_churn_df = kdf1[mask].groupby(['userId']).agg({'ts_date': 'max'}).rename(columns={"ts_date": "ts_churn"}).reset_index()
user_churn_df.head()


# COMMAND ----------

# feature:
# count session ids
# count songs
# count page events
# total song time
# average listening time
# most used user Agent
# count sessions per day
# avg listening time per day
# time from registration to concellation

# COMMAND ----------

user_profile_df = kdf1.groupby('userId').agg({'gender': 'first', 'level':'nunique', 'registration_date' : 'min', 'song': ['count','nunique'], 'sessionId': 'count','length' : 'sum', 'ts_date' :  ['min', 'max']})
user_profile_df.columns =  [' '.join(col).strip() for col in user_profile_df.columns.values]
user_profile_df.head()

# COMMAND ----------

user_profile_df.info()

# COMMAND ----------

user_profile_df['time to first use in days'] = (user_profile_df['ts_date min'] - user_profile_df['registration_date min']) / (3600*24)
user_profile_df['service tenure in days'] = (user_profile_df['ts_date max'] - user_profile_df['ts_date min']) / (3600*24)
user_profile_df.head()

# COMMAND ----------

user_profile_df = user_profile_df.drop(['registration_date min', 'columnts_date min', 'ts_date min', 'ts_date max']).reset_index()
user_profile_df.head()

# COMMAND ----------

merged_df = user_profile_df.merge(user_event_count_df, left_on='userId', right_on='userId')
merged_df.head()

# COMMAND ----------

merged_df.info()

# COMMAND ----------

#merged_df['gender first'] = merged_df['gender first'].map({'M':1,'F':0})
merged_df['churn'] = merged_df['churn'].astype(str)
merged_df.head()

# COMMAND ----------

merged_df.columns = merged_df.columns.str.replace(' ', '_')
merged_df.head()

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table clean_mini_sparkify_event_persist

# COMMAND ----------

permanent_table_name = "clean_mini_sparkify_event_persist"
merged_df[1:].to_table(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from clean_mini_sparkify_event_persist limit 5

# COMMAND ----------

merged_df['churn'].value_counts().plot(kind = 'bar')

# COMMAND ----------

merged_df.columns[2:-1]

# COMMAND ----------

merged_df['gender_first'] = merged_df['gender_first'].astype(int)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

stages = [] # stages in Pipeline

numericCols = list(merged_df.columns[3:-1])
categoricalCol = "gender_first"
labelcol = "churn"

# label
label_stringIdx = StringIndexer(inputCol=labelcol, outputCol="label")
stages += [label_stringIdx]

# categorical feature
stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
ohe_encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
stages += [stringIndexer, ohe_encoder]

assemblerInputs = numericCols
features_assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [features_assembler]

dataset = merged_df.iloc[:,2:].to_spark()

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(dataset)
preppedDataDF = pipelineModel.transform(dataset)

# COMMAND ----------

merged_df.iloc[:,2:].head()

# COMMAND ----------

dataset.printSchema()

# COMMAND ----------

preppedDataDF.printSchema()

# COMMAND ----------

display(preppedDataDF)

# COMMAND ----------

# Fit model to prepped data
lrModel = LogisticRegression().fit(preppedDataDF)
 
# ROC for training data
display(lrModel, preppedDataDF, "ROC")

# COMMAND ----------

display(lrModel, preppedDataDF)

# COMMAND ----------

(trainingData, testData) = preppedDataDF.randomSplit([0.7, 0.3], seed=100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

# Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
 
# Train model with Training Data
lrModel = lr.fit(trainingData)

# COMMAND ----------

# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)

# COMMAND ----------

selected = predictions.select("label", "prediction", "probability")
display(selected)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
 
# Evaluate model
evaluator = BinaryClassificationEvaluator()
print(evaluator.getMetricName(), evaluator.evaluate(predictions))

# COMMAND ----------

print(lr.explainParams())

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
 
rf = RandomForestClassifier(labelCol="label", featuresCol="features")
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10) 
xgb = GBTClassifier(labelCol="label", featuresCol="features")

model_dict = {'rf_classifier': rf, \
               'lr_classifier ': lr, \
               'xgb_classifier ': xgb }

# cross validation for F1 score
auc_roc_results = {} 

for modelname, model in model_dict.items() :
    print ("Training model : {} ...".format(modelname))
    print(model)
      
    # Run cross validations
    current_model = model.fit(trainingData)

    # MLlib will automatically track trials in MLflow. After your tuning fit() call has completed, view the MLflow UI to see logged runs.
    # Use the test set to measure the accuracy of the model on new data
    predictions = current_model.transform(testData)

    # cvModel uses the best model found from the Cross Validation
    # Evaluate best model
    
    auc_roc_results[modelname] =  evaluator.evaluate(predictions) #cvModel.avgMetrics
    print ("Pipeline : {} {} score {}".format(modelname, evaluator.getMetricName(), auc_roc_results[modelname]))

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
#from sparkdl.xgboost import XgboostClassifier

rf = RandomForestClassifier(labelCol="label", featuresCol="features")

paramGrid = (ParamGridBuilder()
             .addGrid(rf.maxDepth, [2, 4, 6])
             .addGrid(rf.maxBins, [20, 60])
             .addGrid(rf.numTrees, [5, 20])
             .build())
 
# Create 5-fold CrossValidator
cv = CrossValidator(estimator=rf, evaluator=evaluator, numFolds=5)
cv.setEstimatorParamMaps(paramGrid)
cv.setParallelism(3)

# Run cross validations
cvModel = cv.fit(trainingData)

# MLlib will automatically track trials in MLflow. After your tuning fit() call has completed, view the MLflow UI to see logged runs.
# Use the test set to measure the accuracy of the model on new data
predictions = cvModel.transform(testData)

# cvModel uses the best model found from the Cross Validation
# Evaluate best model

results = evaluator.evaluate(predictions) #cvModel.avgMetrics
print ("Pipeline : {} {} score {}".format(modelname, evaluator.getMetricName(), results))

# COMMAND ----------

columns = ['level', 'auth', 'method', 'status', 'page']
for col in columns:
  print("col = {} has {} values".format(col, kdf[col].nunique())) 
  print(kdf[col].unique()) 

# COMMAND ----------

kdf['song'].value_counts(ascending=False)

# COMMAND ----------

kdf['artist'].value_counts(ascending=False)

# COMMAND ----------

kdf.groupby('song')['userId'].count().sort_values(ascending=False)

# COMMAND ----------

kdf['location'].value_counts(ascending=False)

# COMMAND ----------

kdf['location'].nunique(), kdf['song'].nunique(), kdf['artist'].nunique()

# COMMAND ----------

level