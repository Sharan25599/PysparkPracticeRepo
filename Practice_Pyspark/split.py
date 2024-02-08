from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark=SparkSession.builder.appName("split_example").getOrCreate()

data=[(101,'Virat Kohli','25-10-2011',2000),
      (101,'Rohit Sharma','11-05-2013',3000),
      (101,'Sachin Tendulkar','21-11-2005',4000),
      (101,'Rahul Dravid','17-03-2006',5000)]
columns=('emp_id','name','DOJ','salary')

df=spark.createDataFrame(data,columns)

# splitted the name column as first_name and last_name
df1= df.withColumn("First Name", split(df['Name'], " ").getItem(0))\
            .withColumn('Last_Name', split(df['Name'], " ").getItem(1))\

df1.show()

# combine multiple split
# slpitted the name column and date of joining column
df2= df.withColumn("first_name",split(df['name']," ").getItem(0))\
       .withColumn("last_name",split(df['name']," ").getItem(1))\
       .withColumn("joining_day",split(df['DOJ'],'-').getItem(0))\
       .withColumn("joining_month",split(df['DOJ'],'-').getItem(1))\
       .withColumn("joining_year",split(df['DOJ'],'-').getItem(2))
df2.show()

# split and drop splitted column
df3=df.withColumn("first_name",split(df['name']," ").getItem(0))\
       .withColumn("last_name",split(df['name']," ").getItem(1))\
       .withColumn("joining_day",split(df['DOJ'],'-').getItem(0))\
       .withColumn("joining_month",split(df['DOJ'],'-').getItem(1))\
       .withColumn("joining_year",split(df['DOJ'],'-').getItem(2))\
       .drop(df['name'])\
       .drop(df['DOJ'])
df3.show()