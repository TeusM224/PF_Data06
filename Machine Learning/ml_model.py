import numpy as np
import pandas as pd
import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics

# LOAD PIPELINE
pipeline = PipelineModel.load('./Models/model_ml_sentiment')
# appName = "Sentiment analysis in Spark"
# sc = SparkSession.builder.appName('Sentiment_Analysis') \
#             .getOrCreate()



# PREDICT FUNCTION
def predict(review):
    prediction = pipeline.transform(review)

    return prediction