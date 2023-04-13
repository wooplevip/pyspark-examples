# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch",2)



columns = ["name","currentState"]
data = [("James,,Smith","CA"), \
    ("Michael,Rose,","NJ"), \
    ("Robert,Williams","NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.printSchema()
df.show(truncate=False)

def fun(iterator):
    for f in iterator:
        pdf=pd.DataFrame(f)
        pdf['name']='foo'
        print("====")
        print(pdf.shape[0])
        yield pdf

df.mapInPandas(fun, "name string, currentState string").show()


# from pyspark.sql.functions import col, concat_ws
# df2 = df.withColumn("languagesAtSchool",
#    concat_ws(",",col("languagesAtSchool")))
# df2.printSchema()
# df2.show(truncate=False)


# df.createOrReplaceTempView("ARRAY_STRING")
# spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool,currentState from ARRAY_STRING").show(truncate=False)
