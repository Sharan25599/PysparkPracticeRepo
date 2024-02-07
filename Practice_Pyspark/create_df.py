from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()
# Example1:
data=[["Hemanth",24],["Rahul",25],["Barath",24]]
df= spark.createDataFrame(data,['name','age'])

df.show()

# Example2
data=[['Virat',39],['Dhoni',43],['Rohit',38]]
columns=['name','age']
df=spark.createDataFrame(data=data,schema=columns)
df.show()

# use to select the column
df_age=df.select("age")
df_age.show()

df_multi=df.select('name','age')
df_multi.show()

