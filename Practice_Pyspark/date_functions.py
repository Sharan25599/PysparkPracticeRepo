from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date,date_format,to_date

spark =SparkSession.builder.appName("UnionByName Example").getOrCreate()

# Create DataFrames with the same schema
data=[(101,'ABC'),(102,'XYZ'),(103,'PQR')]
df= spark.createDataFrame(data, ["id","name"])

df.show()
df.printSchema()

# It gives the current date and format in default dateformat (yyyy-MM-dd)
df1 = df.withColumn('currentDate', current_date())
df1.show()

# It will convert the date format to a specified format but the datetype will be string
df2 = df1.withColumn('currentDate', date_format(df1.currentDate, 'dd.MM.yyyy'))
df2.show()

# It will convert date from stringtype to datetype,but in this format(yyyy-MM-dd)
df3=df2.withColumn('currentDate',to_date(df2.currentDate,'dd.MM.yyyy'))
df3.show()


from pyspark.sql.functions import datediff,months_between,add_months,date_add,year,month,day

df=spark.createDataFrame([('2023-10-19','2023-11-19')],['d1','d2'])
df.show()

# It gives the diff between the dates(in days)
df.withColumn('dateDiff',datediff(df.d2,df.d1)).show()

# It gives the diff between the months(1 month for above specified dates)
df.withColumn('monthsBetween',months_between(df.d2,df.d1)).show()

# It will add months to the specified date
df.withColumn('addMonths',add_months(df.d2,3)).show()

# It will subtract the months to the specified date
df.withColumn('SubtractMonths',add_months(df.d2,-3)).show()

# It will add days to the specified date
df.withColumn('addDays',date_add(df.d1,10)).show()

# It will subtract the days to the specified date
df.withColumn('addDays',date_add(df.d1,-10)).show()

# It gives year from a specified date
df.withColumn('year',year(df.d2)).show()

# It gives month from a specified date
df.withColumn('month',month(df.d2)).show()

# It gives day from a specified date
df.withColumn('day',day(df.d2)).show()

