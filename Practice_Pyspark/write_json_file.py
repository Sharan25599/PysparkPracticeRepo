from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark=SparkSession.builder.appName("json file").getOrCreate()

data=[(1,'Sharan'),(2,'Sathish')]
schema=['id','name']

df=spark.createDataFrame(data,schema)
df.show()

# df.write.json(path=r"C:\Users\SharanKumar\Desktop\csv\json")
df.write.format('json').mode('append').save(r'C:\Users\SharanKumar\Desktop\csv\json')
# spark.read.json(path=r'C:\Users\SharanKumar\Desktop\csv\json')