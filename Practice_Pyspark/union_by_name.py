from pyspark.sql import SparkSession


spark =SparkSession.builder.appName("UnionByName Example").getOrCreate()

# Create DataFrames with the same schema
df1 = spark.createDataFrame([(101,"Rahul", 25), (102,"Barath", 30)], ["id","name", "age"])
df2 = spark.createDataFrame([(201,"Kevin", 3000), (202,"Sathya",2000)], ["id","name", "salary"])
df1.show()
df2.show()

# It will merge the dataframe based on the columns name
# when we have different schema we can merge/union by using unionByName()
df1.unionByName(df2,allowMissingColumns=True).show()