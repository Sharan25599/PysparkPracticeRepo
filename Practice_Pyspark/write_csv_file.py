from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("write example").getOrCreate()

data=[('Sharan',29),('Harish',32)]
column=['name','id']
df=spark.createDataFrame(data,column)
# df.show()

df.write.csv(path=r"C:\Users\SharanKumar\Desktop\csv1",mode='append')
df.show()

# df=spark.read.csv(path=r"C:\Users\SharanKumar\Desktop\csv1",header=True)