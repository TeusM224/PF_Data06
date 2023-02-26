"""
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://apache.osuosl.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop2.tgz 
!tar xf spark-3.3.2-bin-hadoop2.tgz 
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.2-bin-hadoop2"
"""

import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover

#Create_Spark_session
appName = "Sentiment analysis in Spark"
spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Importing_data
path = '/content/drive/MyDrive/sentiment.csv'
rewiews_csv = spark.read.option("delimiter", ";").option("header", "true").csv(path)
rewiews_csv.show(truncate=False, n=5)

#set_nan_values
rewiews_csv = rewiews_csv.na.fill("not commented", 'text')

#Selecting_columns_to_work
data = rewiews_csv.select('text','label')
data.show(n=5)

#deleting_special_characters_and_emojis
import re
data.toDF(*[re.sub('[\)|\(|\s|,|%]','',x) for x in data.columns])

data = data.withColumn(
    "text",
    F.regexp_replace(F.regexp_replace("text", "[^\x00-\x7F]+", ""), '""', '')
)

#Start_Sentiment_analyst
splitData = data.randomSplit([0.6, 0.4]) 
trainingData = splitData[0] 
testingData = splitData[1] 
train_rows = trainingData.count()
test_rows = testingData.count()
print ("Training data rows:", train_rows, "; Testing data rows:", test_rows)


trainingData =  trainingData.withColumn("label",col("label").cast("Integer"))

#STAGES_tokenizer_StopWordsRemover_HashingTF
tokenizer = Tokenizer(inputCol="text", outputCol="sentimentText")
tokenizedTrain = tokenizer.transform(trainingData)
tokenizedTrain.show(truncate=False, n=5)

swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), 
                       outputCol="MeaningfulWords")
SwRemovedTrain = swr.transform(tokenizedTrain)
SwRemovedTrain.show(truncate=False, n=5)

hashTF = HashingTF(inputCol=swr.getOutputCol(), outputCol="features")
numericTrainData = hashTF.transform(SwRemovedTrain).select(
    'label', 'MeaningfulWords', 'features')
numericTrainData.show(truncate=False, n=5)

#Train_our_classifier_model_using_training_data
lr = LogisticRegression(labelCol="label", featuresCol="features", 
                        maxIter=10, regParam=0.01)
model = lr.fit(numericTrainData)

#Prepare_testing_data
tokenizedTest = tokenizer.transform(testingData)
SwRemovedTest = swr.transform(tokenizedTest)
numericTest = hashTF.transform(SwRemovedTest).select(
    'Label', 'MeaningfulWords', 'features')
numericTest.show(truncate=False, n=2)

#Predict_testing_data_and_calculate_the_accuracy_model
prediction = model.transform(numericTest)
predictionFinal = prediction.select(
    "MeaningfulWords", "prediction", "Label")
predictionFinal.show(n=4, truncate = False)
correctPrediction = predictionFinal.filter(
    predictionFinal['prediction'] == predictionFinal['Label']).count()
totalData = predictionFinal.count()
print("correct prediction:", correctPrediction, ", total data:", totalData, 
      ", accuracy:", correctPrediction/totalData)

#Pipeline_with_model
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

pipeline = Pipeline(stages=[tokenizer,swr ,hashTF, lr])

model = pipeline.fit(trainingData)

model.save('/content/drive/MyDrive/model_ml_sentiment')



