from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data=[('Sharan',24,'M'),('Sharukesh',23,'M'),('Thashwanth',21,'M')]
columns=('name','age','gender')

df=spark.createDataFrame(data,columns)

df_drop=df.drop('gender')
df_drop.show()