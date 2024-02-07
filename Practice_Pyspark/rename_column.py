from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data=[('John',24,'M'),('Joesph',23,'M'),('Baldwin',21,'M')]
columns=('name','age','gender')

df=spark.createDataFrame(data,columns)

# renamed old col name to new col name
df_rename=df.withColumnRenamed('name','Emp_name')
df_rename.show()