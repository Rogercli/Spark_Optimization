

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os

spark = SparkSession.builder.appName('Optimize I').getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 16)
answers_input_path = "/home/roger/SB/Spark/Spark_Optimization/data/answers"

answersDF = spark.read.option('path', answers_input_path).load()
questions_input_path = "/home/roger/SB/Spark/Spark_Optimization/data/questions"
questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''



answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF= questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

