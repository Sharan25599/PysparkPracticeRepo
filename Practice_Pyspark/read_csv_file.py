from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

spark=SparkSession.builder.appName("csv file").getOrCreate()

# # Read a csv file
# df=spark.read.csv(path=r"C:\Users\SharanKumar\Desktop\New folder\Emp1.csv",header=True)
# df.show()
# df.printSchema()


# # read multiple csv file
# df=spark.read.csv(path=[r"C:\Users\SharanKumar\Desktop\New folder\Emp1.csv",r"C:\Users\SharanKumar\Desktop\New folder\Emp2.csv"],header=True)
# df.show()
# df.printSchema()
#
# df_1=spark.read.format('csv').option(key='header',value=True).load(path=r"C:\Users\SharanKumar\Desktop\New folder\Emp1.csv")
# df_1.show()

# custom schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    ])

df=spark.read.csv(path=[r"C:\Users\SharanKumar\Desktop\New folder\Emp1.csv",r"C:\Users\SharanKumar\Desktop\New folder\Emp2.csv"],schema=schema,header=True)
df.show()
df.printSchema()

# sort the data according to id
df1=df.orderBy(df.id)
df1.show()

# changed the column name from 'name' to 'Stud_name'
df2=df1.withColumnRenamed('name','Stud_name')
df2.show()

df3=df2.withColumn('salary',lit('2000'))
df3.show()

# df3=df2.groupBy('Stud_name','gender').count()
# df3.show()




