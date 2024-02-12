from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("WhereExample").getOrCreate()

data=[('John',24,'M'),('Joesph',23,'M'),('Baldwin',21,'M')]
columns=('name','age','gender')

df=spark.createDataFrame(data,columns)

#apply where condition for age
df_fiter=df.where(df.age >= 20)
df_fiter.show()

#apply where condition for age and gender
df_filter1=df.where((df.age >= 25) & (df.gender == 'M'))
df_filter1.show()


# using Filter()

data=[(10,'Rahul',100,'M',2000),(20,'Barath',101,'M',4000),(30,'Balaji',102,'M',5000),(40,'Pooja',103,'F',3000)]
schema=('id','name','emp_dept_id','gender','salary')

df = spark.createDataFrame(data,schema)

# filter the salary column in which sal is greater then and equal to 2500
df.filter(df.salary >=2500).show()

#filter the gender where gender equal to female
df.filter(df.gender == 'F').show()

# filter based upon gender is male and salary is 4000
df.filter((df.gender=='M') &(df.salary== 4000)).show()

# checks whether the salary column is null
df.filter(df.salary.isNull()).show()

# checks whether the salary column is notnull
df.filter(df.salary.isNotNull()).show()

# filter name end with character 'l' using like()
df.filter(df.name.like('%l')).show()

# filter name ends with characters 'ja' using endswith()
df.filter(df.name.endswith('ja')).show()

# filter name starts with characters 'Ba' using startswith()
df.filter(df.name.startswith('Ba')).show()

# we can check the value is present in the table using isin()
df.filter(df.id.isin(10,30)).show()



