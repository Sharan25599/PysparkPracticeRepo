from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("join_example").getOrCreate()

emp_data=[(11,'Virat Kohli','2011',100,'M',2000),
      (12,'Rohit Sharma','2013',200,'M',3000),
      (13,'Sachin Tendulkar','2005',300,'M',4000),
      (14,'Rahul Dravid','2006',400,' ',5000)]
columns=('emp_id','name','YOJ','dept_id','gender','salary')

df1=spark.createDataFrame(emp_data,columns)
# df1.show()

dept_data=[('HR',100),('Senior',200),('Junior',300)]
schema=('dept_name','dept_id')
df2=spark.createDataFrame(dept_data,schema)
# df2.show()

# It returns all the rows which are matching in both tables
df_inner=df1.join(df2,on='dept_id',how='inner')
df_inner.show()
 # df1.join(df2,df1.emp_dept_id == df2.dept_id,'inner')

# It returns all rows from the left dataframe and matching rows from the right dataframe
df_left=df1.join(df2,on='dept_id',how='left')
df_left.show()

#It returns all rows that is matching and unmatching from both tables
df_full=df1.join(df2,on='dept_id',how='full')
df_full.show()

# It returns all rows from the right dataframe and matching rows from the left dataframe
df_right=df1.join(df2,on='dept_id',how='right')
df_right.show()

# It returns all rows from the left dataframe and where there is match in right dataframe
df_semi=df1.join(df2,on='dept_id',how='semi')
df_semi.show()

# It returns all rows from the left dataframe and where there is no match in right dataframe
df_anti=df1.join(df2,on='dept_id',how='anti')
df_anti.show()

