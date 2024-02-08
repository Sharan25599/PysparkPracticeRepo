from pyspark.sql import SparkSession


spark =SparkSession.builder.appName("UnionExample").getOrCreate()

# Create DataFrames with the same schema
df1 = spark.createDataFrame([("Rahul", 25), ("Barath", 30)], ["name", "age"])
df2 = spark.createDataFrame([("Kevin", 28), ("Sathya", 35)], ["name", "age"])

# Use union to combine DataFrames and remove duplicates
df_combined = df1.union(df2)
df_combined.show()

# Using unionAll will keep duplicates (deprecated):
df_combined_all = df1.unionAll(df2)
df_combined_all.show()

# To only remove duplicates manually:
df_combined_all_distinct = df_combined_all.distinct()
df_combined_all_distinct.show()
