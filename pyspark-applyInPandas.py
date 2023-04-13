import pyspark
from pyspark.sql import SparkSession
import pandas as pd 

spark = SparkSession.builder.master("local[1]") \
                    .appName('example') \
                    .getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
  
  
df = spark.createDataFrame(data=simpleData, schema = schema)
#df.printSchema()
#df.show(truncate=False)

def fun(key, pdf):
    print(key)
    s=pdf['salary'].sum()
    label = 0
    if(s>10000):
        label = 1
    return pd.DataFrame([key + (s,label,)])

rdf = df.groupBy("department").applyInPandas(fun, schema="department string, sum int, lable int")
rdf.show()
