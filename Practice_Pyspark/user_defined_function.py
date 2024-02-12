from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UDF Example").getOrCreate()

data = [('id', 'Sharan', 2000, 500), ('id', 'Sharukesh', 2500, 1000)]
columns = ('id', 'name', 'salary', 'bonus')

df = spark.createDataFrame(data, columns)
df.show()


def totalpay(s, b):
    return s + b


from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

TotalPyment = udf(lambda s, b: totalpay(s, b), IntegerType())
df.withColumn('Totpay',TotalPyment(df.salary,df.bonus)).show()

