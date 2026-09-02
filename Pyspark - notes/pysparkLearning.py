# Databricks notebook source
# DBTITLE 1,Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Employee_ETL") \
    .getOrCreate()





# COMMAND ----------

# DBTITLE 1,Read Source File
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/myvolume/data/employees.csv")

df.show()




# COMMAND ----------

# DBTITLE 1,Check Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records
df = df.dropDuplicates()




# COMMAND ----------

df.show()



# COMMAND ----------

# DB TITLE 1,Check Null Values

from pyspark.sql.functions import col, count, when

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()



# COMMAND ----------

# DBTITLE 1,Handle Null Values
df = df.fillna({
    "dept": "UNKNOWN",
    "salary": 0
})

df.show()



# COMMAND ----------

# DB TITLE 1,Data Type Casting
from pyspark.sql.functions import col

df = df.withColumn(
    "salary",
    col("salary").cast("double")
)



# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Filter Data
it_df = df.filter(
    col("dept") == "IT"
).show()



# COMMAND ----------

# DBTITLE 1,Add Derived Columns
from pyspark.sql.functions import lit

df = df.withColumn(
    "bonus",
    col("salary") * lit(0.10)  # lit used for constant value
)

df.show()



# COMMAND ----------

# DBTITLE 1,Add Audit Columns
from pyspark.sql.functions import current_timestamp,current_date

df = df.withColumn(
    "load_timeStamp",
    current_timestamp()    
).withColumn(
    "load_date",
    current_date()

)

df.show()

# COMMAND ----------

# DBTITLE 1,Sort Data
df = df.orderBy(
    col("salary").desc()
)
df.show()

# COMMAND ----------

# DBTITLE 1,Aggregation
from pyspark.sql.functions import sum, min, max, mean

dept_salary = df.groupBy("dept") \
    .sum("salary").alias("total_salary") 
dept_salary.show()

# COMMAND ----------

# DBTITLE 1,Aggregation
from pyspark.sql.functions import sum, min, max, mean

dept_salary = df.groupBy("dept") \
    .agg(
        sum("salary").alias("total_salary"),
        min("salary").alias("min_salary"),
        max("salary").alias("max_salary"),
        mean("salary").alias("avg_salary")
    )

dept_salary.show()

# COMMAND ----------

# DBTITLE 1,Window Function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy(col("salary").desc())

df_rank = df.withColumn(
    "rn",
    row_number().over(window_spec)
)

top_emp = df_rank.filter(
    col("rn") == 2
)

top_emp.show()

# COMMAND ----------

# DBTITLE 1,Window Function
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

window_spec = Window.orderBy(col("salary").desc())

df_rank = df.withColumn(
    "dr",
    dense_rank().over(window_spec)
)

top_emp = df_rank.filter(col("dr") == 2)

top_emp.show()




# COMMAND ----------

# DBTITLE 1,Window Function dept wise

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("dept") \
                    .orderBy(col("salary").desc())

df_rank = df.withColumn(
    "rn",
    row_number().over(window_spec)
)

top_emp = df_rank.filter(
    col("rn") == 1
)

top_emp.show()

# COMMAND ----------

# DBTITLE 1,Join
dept_df = spark.createDataFrame([
    (1, "IT"),
    (2, "HR"),
    (3, "FINANCE")
], ["dept_id", "dept"])

final_df = df.join(
    dept_df,
    "dept",
    "left"
)
df.show()
final_df.show()

# COMMAND ----------

# DBTITLE 1,Write Data to Parquet
final_df.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/myvolume/output/employees")

# COMMAND ----------

# DBTITLE 1,Incremental Load Pattern
from pyspark.sql.functions import max

last_loaded_date = "2026-06-10"

incremental_df = df.filter(
    col("load_date") > last_loaded_date
)

incremental_df.show()

# COMMAND ----------

# DBTITLE 1,count
record_count = df.count()

print(f"Source Record Count: {record_count}")




# COMMAND ----------

jdbc_url = "jdbc:oracle:thin:@//localhost:1521/xe"

oracle_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "EMPLOYEES") \
    .option("user", "hr") \
    .option("password", "hr") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    .load()

oracle_df.show()