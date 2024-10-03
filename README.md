# PySpark
All I know about PySpark

For all the code executions above, the one time code is

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import os
from pyspark.sql.functions import *

# ======================================================================================
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'Java path' # Please write the java path here as per your laptop
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# ======================================================================================
