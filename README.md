# PySpark
All I know about PySpark

For all the code executions above, Please put the below code at the begining

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import os
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'Java path' # Please write the java path here as per your laptop
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

If you truly want to learn Big data tools like Hadoop, Sqoop, Hive, Spark and Cloud counterparts of them. Please ping me on whatsapp 9490716829.

https://github.com/user-attachments/assets/b73bda8c-2233-4660-8678-1bea82d4736f
