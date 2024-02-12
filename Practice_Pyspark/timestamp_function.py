from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, hour, minute, second, lit

spark=SparkSession.builder.appName("timestamp Example").getOrCreate()


data=[(101,'ABC'),(102,'XYZ'),(103,'PQR')]
df= spark.createDataFrame(data, ["id","name"])

df.withColumn('currentTimestamp',current_timestamp()).show(truncate=False)
df.printSchema()

df1=df.withColumn('timestampInstring',lit('12.25.2023 08.10.03'))
df1.show(truncate=False)
df1.printSchema()

df2=df1.withColumn('timestampInstring',to_timestamp(df1.timestampInstring,'MM.dd.yyyy HH.mm.ss'))
df2.show(truncate=False)
df2.printSchema()

df.select('*',hour(df.currentTimestamp).alias('hour'),\
    minute(df.currentTimestamp).alias('minute'),\
    second(df.currentTimestamp).alias('second')).show(truncate=False)

