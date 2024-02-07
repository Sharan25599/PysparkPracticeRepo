from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("WhereExample").getOrCreate()

data=[('John',24,'M'),('Joesph',23,'M'),('Baldwin',21,'M')]
columns=('name','age','gender')

df=spark.createDataFrame(data,columns)

#apply where condition for age
df_fiter=df.where(df.age >= 20)
df_fiter.show()

#apply where condition for age and gender
df_filter1=df.where((df.age >= 25) & (df.gender == 'M'))
df_filter1.show()

