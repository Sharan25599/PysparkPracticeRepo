from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark=SparkSession.builder.getOrCreate()

data = [
    ("Harish", 32),
    ("Nithesh", 23),
    ("Sachin", 27)
]
columns=['name','age']
df=spark.createDataFrame(data=data,schema=columns)

df_add=df.withColumn('city', lit("Chennai"))
df_add.show()

# just updated age without newly added column
df_update=df.withColumn('age',df.age+5)
df_update.show()

# updated after adding new column (display with new column)
df_update=df_add.withColumn('age',df_add.age+5)
df_update.show()