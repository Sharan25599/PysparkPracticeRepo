from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("orderby_sort example").getOrCreate()


data=[(1,'Rahul',2000,'HR'),(2,'Barath',4000,'IT'),(3,'Balaji',5000,'payroll'),(4,'Rohit',3000,'HR')]
schema=('id','name','salary','dept')

df=spark.createDataFrame(data,schema)
# df.show()

df.orderBy(df.dept,df.id).show()

df.sort(df.dept.desc(),df.id.desc()).show()

