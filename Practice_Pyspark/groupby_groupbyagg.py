from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("groupBy_sort example").getOrCreate()


data=[(1,'Rahul',2000,'HR','M'),(2,'Barath',4000,'IT','M'),(3,'Balaji',5000,'payroll','M'),(4,'Rohit',3000,'HR','M'),(5,'Pooja',3500,'sales','F')]
schema=('id','name','salary','dept','gender')

df=spark.createDataFrame(data,schema)

# number of dept
df.groupBy('dept').count().show()

# Getting highest salary in each dept
df.groupBy('dept').max('salary').show()

# Getting lowest salary in each dept
df.groupBy('dept').min('salary').show()

# to find genders in each dept
df.groupBy('dept','gender').count().show()

from pyspark.sql.functions import count,max,min

df.groupBy('dept').agg(count('*').alias('CountofEmp')).show()

# employees in each dept with they min and max sal
df.groupBy('dept').agg(count('*').alias('CountofEmp'),\
                  min('salary').alias('MinSal'),\
                  max('salary').alias('MaxSal')).show()

