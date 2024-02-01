create_DataFrame.py
- using createDataFrame() created column (name and age)
  
drop_column.py
- Drop(): it is used to remove the column in DataFrame
- using Drop() gender and age columns are Removed

groupBy.py
- GroupBy(): used to group rows in a DataFrame based on one or more specified columns.
             It allows you to perform aggregation functions on the grouped data, such as calculating 
             counts, sums, averages.
-using groupBy() city and item as be grouped in DataFrame.
(grouped_city_df = sales_df.groupBy("city")) - Single column
(grouped_city_item_df = sales_df.groupBy("city", "item")) - Multiple columns

orderBy_sort.py
- OrderBy():It takes a list of columns as its arguments and returns a new DataFrame that is sorted by the                
            specified columns. (df.orderBy("age"))
- sort(): It is used to sort on one or more columns.
          It takes a Boolean argument for ascending (or) descending order.(df.sort("age"))

rename_nested_column.py
-withColumnRenamed: It is used to rename a specific column within a DataFrame. It takes two arguments:
                  
                    existing: The name of the column you want to rename (a string).
                    new: The new name you want to give to the column (a string).
                    
                    It returns a new DataFrame with the specified column renamed, leaving the original DataFrame       
                    untouched.
                    (df.withColumnRenamed("address.street", "address.new_street") 

select.py
-Select(): It is used to select specific column from DataFrame(df.select("name"))

select_multiple_col.py
-Used to select multiple columns using select()
 (df_selected1 = df.select("name", "age"))

structType_structField.py
-StructType: Represents the overall structure of the DataFrame, similar to a database table schema.
-StructField:Represents an individual column within the DataFrame.
             Defined by:
             name (string): Name of the column.
             dataType (DataType object): Data type of the column (e.g., IntegerType, StringType).
             nullable (boolean, optional): Whether the column can contain null values (default: True).

             schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

joins.py
-Inner Join: Returns only the rows with matching keys in both DataFrames.
-Left Join:  Returns all rows from the left DataFrame and matching rows from the right DataFrame.
-Right Join: Returns all rows from the right DataFrame and matching rows from the left DataFrame.
-Full Outer Join: Returns all rows from both DataFrames, including matching and non-matching rows.
-Left Semi Join:  Returns all rows from the left DataFrame where there is a match in the right DataFrame.
-Left Anti Join:  Returns all rows from the left DataFrame where there is no match in the right DataFrame.

union_unionall.py
union:Returns a new DataFrame containing unique rows from the input DataFrames.
      (df_combined = df1.union(df2))
unionall: Returns a new DataFrame containing all rows from the input DataFrames, including duplicates.
          (df_combined_all = df1.unionAll(df2))
          
          To remove duplicates manually: using distinct()
          (df_combined_all_distinct = df_combined_all.distinct())
