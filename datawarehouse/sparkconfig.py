!apt-get install openjdk-11-jdk-headless -qq > /dev/null #Instala la version de Java sobre la que se soporta Spark
!wget -q http://apache.osuosl.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz #descarga de la pagina de Apache la version de spark 3.2.3 y Hadoop 3.2 (Ambas son de a finales del año pasado)
!tar xf spark-3.2.3-bin-hadoop3.2.tgz #Descomprime el archivo
!pip install -q findspark #Instala findspark que detecta el programa en la maquina virtual

#Se configuran las variables de entorno para la ejución de Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.3-bin-hadoop3.2"

#Creamos una sesión de prueba para verificar que esta funcionando
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext

sqlSession = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sc

# Otra sesion para inicializar, esta es la usada para el proceso de Sentiment Analyst

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover

appName = "Sentiment Analysis Maps in Spark"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

