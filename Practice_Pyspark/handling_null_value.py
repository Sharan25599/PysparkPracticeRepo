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
# df.show()
#
# # It gives all rows having null values
# df.filter(df.marks.isNull()).show()
#
# # It gives all rows which does not have null values
# df.filter(df.marks.isNotNull()).show()

# # It gives rows which having null values in multiple columns
# df.filter((df.marks.isNull()) & df.status.isNull()).show()
#
# # It checks either of the columns and gives the rows having null values
# df.filter((df.marks.isNull()) | df.status.isNull()).show()
#
# # drop the row contains null values
# df.na.drop().show()
# # or
# df.na.drop('any').show()

# Alternate syntax for drop a row contains null values
# df.dropna().show()

# # remove the row having null values in all the column
# df.na.drop('all').show()

# Removes all the rows having null in a particular column
# df.na.drop(subset='marks').show()
#
# # Removes all the rows having null in multiple columns column
# df.na.drop(subset=['marks','status']).show()
#
# # It replaces the null values with 0 only in integer columns 'marks'
# df.na.fill(value=0).show()
#
# # It replaces the null values with 'NA' only in string columns 'name','subject','Status'
# df.na.fill(value='NA').show()

# it replaces the null value based on the particular column/multiple columns as we mention in subset
df.na.fill(value=0,subset=['marks']).show()
df.na.fill(value=0,subset=['marks','id']).show()

# replacing the null values in all columns with any other values
df.na.fill({'marks':0, 'status':'NA', 'subjects':'Hindi','id':16,'name':'no_name'}).show()