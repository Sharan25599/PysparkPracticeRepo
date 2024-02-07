from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark=SparkSession.builder.appName("Structtype_Structfield").getOrCreate()

data= [("James", "", "Smith", 101, "M", 3000),
        ("Michael", "Rose", "", 102, "M", 4000),
        ("Robert", "", "Williams", 103, "M", 4000),
        ("Maria", "Anne", "Jones", 104, "F", 4000),
        ("Jen", "Mary", "Brown", 105, "F", -1)
        ]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
    ])

df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()

#nested Structure
dataStruct=[(('Hemanth','Kumar'),'25-05-1999','M','2000'),
            (('Sharan',''),'24-12-1999','M','2500'),]

# used nested schemaStruct
schemaStruct = StructType([
        StructField('name  ', StructType([
             StructField('firstname', StringType(), True),
             StructField('lastname', StringType(), False)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])
df = spark.createDataFrame(data=dataStruct, schema = schemaStruct)
df.printSchema()
df.show()

