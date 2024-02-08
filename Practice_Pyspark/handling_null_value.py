from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Null_example").getOrCreate()

data=[(11,'Sharan','Maths',88,'p'),
      (12,'Rahul','Social',90,'p'),
      (13,'Harikanth','Kannada',None,'f'),
      (14,'Kevin','Science',80,'p'),
      (15,'Balaji','English',None,None),
      (None,None,None,None,None)]
columns=('id','name','subjects','marks','status')

df=spark.createDataFrame(data,columns)
df.show()

# It gives all rows having null values
df.filter(df.marks.isNull()).show()

# It gives all rows which does not have null values
df.filter(df.marks.isNotNull()).show()

# It gives rows which having null values in multiple columns
df.filter((df.marks.isNull()) & df.status.isNull()).show()

df.filter((df.marks.isNull()) | df.status.isNull()).show()

# drop the row contain null values
df.na.drop().show()
# or
df.na.drop('any').show()
