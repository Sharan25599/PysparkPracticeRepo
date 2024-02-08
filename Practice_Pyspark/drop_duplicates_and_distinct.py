from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data=[(111,'Sharan',88),(222,'Sharukesh',99),(111,'Sharan',88),(222,'Sharukesh',78)]
columns=('id','name','score')

df=spark.createDataFrame(data,columns)
df.show()

# It is used to remove duplicate rows in the dataframe(removed a row(111,Sharan,88))
df.distinct().show()

# It will remove the duplicates but it exclude the score column, based on duplicate values(id,name))
df.select('id','name').distinct().show()

# distinct and dropduplicates acts as same when we does not give input parameter
df.dropDuplicates().show()

# It will remove the duplicates values based on (id,name), but does exclude the score column
df.dropDuplicates(['id','name']).show()



