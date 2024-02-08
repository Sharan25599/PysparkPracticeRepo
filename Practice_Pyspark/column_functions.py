from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("ColFuncExamples").getOrCreate()

data=[(1,'Rahul',2000),(2,'Barath',4000),(3,'Balaji',5000),(4,'Rohit',3000)]
schema=('id','name','salary')

df=spark.createDataFrame(data,schema)

# alias()-used to change th column name
df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.salary.alias('emp_sal')).show()

# asc()-sorting in ascending order based on the name column
df.sort(df.name.asc()).show()

# desc()-sorting in descending order based on the salary column
df.sort(df.salary.desc()).show()


df.select(df.id,df.name,df.salary)
df.printSchema()

# changed the datatype of id long to float
df1=df.select(df.salary.cast('float'))
df1.select('salary').show()
df1.printSchema()

# changed the datatype of id long to integer
df2=df.select(df.id.cast('int'))
df2.printSchema()

# filter the name starts with 'R' using like()
df2=df.filter(df.name.like('R%'))
df2.show()