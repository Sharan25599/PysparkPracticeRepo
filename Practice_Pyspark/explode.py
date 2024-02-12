from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,explode_outer,posexplode

spark=SparkSession.builder.appName('ArrayType').getOrCreate()

data=[('Hemanth',['TV','refrigerator','oven','AC']),
      ('Rahul',['AC','Washing Machine',None]),
      ('Sathya',['Grinder','TV']),
      ('Rajesh',None)]
schema=['name','appliance']
df=spark.createDataFrame(data,schema)
df.printSchema()
df.show()

# It returns new row for each element in the give array
df1=df.select(df.name,explode(df.appliance))
df1.show()
df1.printSchema()

# It includes the row which contains null values
df.select(df.name,explode_outer(df.appliance)).show()

# It creates positional column for each element
df.select(df.name,posexplode(df.appliance)).show()