import streamlit as st
import numpy as np
import pandas as pd
import findspark
findspark.init()
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from PIL import Image
import ml_model


positive = Image.open('positive.png')
negative = Image.open('negative.png')

#create Spark session
appName = "Sentiment analysis APP"
sc = SparkSession.builder.appName('Sentiment_Analysis_App') \
            .getOrCreate()

st.title('Machine Learning Model')
st.markdown('### Supervised Clasification Model With Logistic Regression ###')
#Google Cloud Datawarehouse connection

# INPUT THE REVIEW TO RUN IT WITH THE MODEL
review = st.text_input('Write your review in English', '', max_chars=200)
dict = Row(review)
review_pred = sc.createDataFrame([dict], ["text"])

if review != '':
    
    pred = ml_model.predict(review_pred)
    prediction = pred.select('text', "prediction")
    
    prediction_df = prediction.toPandas()
    
    prediction_df['prediction'] = prediction_df['prediction'].astype('int')
    
    st.markdown('#')
    col1, col2 = st.columns(2)
    with col1:
        if prediction_df.prediction[0] == 1:
            st.subheader('This is a Positive Review')
        else:
            st.subheader("This is a Negative Review")

    with col2:
        if prediction_df.prediction[0] == 1:
            st.image(positive)
        else:
            st.image(negative)
        st.markdown('#')
        st.markdown('#')

    




