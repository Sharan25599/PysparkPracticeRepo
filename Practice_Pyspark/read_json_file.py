from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,explode_outer,posexplode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType

spark=SparkSession.builder.appName("json file").getOrCreate()

# Read a json file
df=spark.read.json(path=r"C:\Users\SharanKumar\Desktop\JSON files\sample2.json",multiLine=True)

df.printSchema()
df.show()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    ])

df=spark.read.json(path=r"C:\Users\SharanKumar\Desktop\JSON files\sample1.json",schema=schema,multiLine=True)

df.printSchema()
df.show()

df=spark.read.json(path=r"C:\Users\SharanKumar\Desktop\JSON files\sample2.json",multiLine=True)
df.printSchema()
df.show()

df=spark.read.json(path=[r"C:\Users\SharanKumar\Desktop\JSON files\sample2.json",r"C:\Users\SharanKumar\Desktop\JSON files\sample2.json"],multiLine=True)

df.printSchema()
df.show()

# read all the json files from the specified folder
df=spark.read.option('multiline',True).json(path=r"C:\Users\SharanKumar\Desktop\JSON files\*")

df.printSchema()
df.show()


# manipulating the json file
df1=spark.read.json(r"C:\Users\SharanKumar\Desktop\JSON files\animals-1.json",multiLine=True)
df1.printSchema()
df1.show()

# sort by species
df1.orderBy('species').show()
# rename the column
df1.withColumnRenamed('name','Animal_name').show()

# schema contains with ArrayType
custom_schema=StructType([
    StructField("name", StringType(), nullable=True),
    StructField("species", StringType(), nullable=True),
    StructField("foods", StructType([
        StructField("dislikes", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("likes", ArrayType(StringType(), containsNull=True), nullable=True)
    ]), nullable=True)
])

df1=spark.read.json(r"C:\Users\SharanKumar\Desktop\JSON files\animals-1.json",multiLine=True,schema=custom_schema)
df1.show()
df1.printSchema()

# explode - When array is passed to this function it creates new row for each element in array
df2=df1.select('name','species','foods.*')
df2.show()

# explode_outer - if array is null, it returns the null
df1.select("name","species",explode_outer('foods.likes')).show()

# posexplode - if array is passed it creates positional column for each element and it ignores the null value
df1.select("name","species",posexplode('foods')).show()

