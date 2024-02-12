from pyspark.sql import  SparkSession

spark=SparkSession.builder.appName('pivot,unpivot Example').getOrCreate()

data=[('ABC','Q1',2000),
      ('ABC','Q2',6000),
      ('ABC','Q3',4000),
      ('ABC','Q4',5000),
      ('XYZ','Q1',2000),
      ('XYZ','Q2',4000),
      ('XYZ','Q3',5000),
      ('XYZ','Q4',6000),
      ('KLM','Q1',7000),
      ('KLM','Q2',4000),
      ('KLM','Q3',2000),
      ('KLM','Q4',1000)]
schema=['company','Quarter','Revenue']
df=spark.createDataFrame(data,schema)
df.show()

pivot_df=df.groupby('company').pivot('Quarter').sum('Revenue')
pivot_df.show()

df=pivot_df.selectExpr('company',"stack(4,'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4) as (Quarter,Revenue)")
df.show()