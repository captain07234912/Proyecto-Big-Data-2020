# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Proyecto Big data 2020
# MAGIC  ##### Universidad del Valle de Guatemala  
# MAGIC ##### Jorge Súchite 
# MAGIC ##### Carnet 15293
# MAGIC  ##### 20/11/2020
# MAGIC  
# MAGIC  
# MAGIC Multas de tránsito en New York 

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/ParkingTotal2-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Registration_State,   count(Summons)  from `Data` group by Registration_State
# MAGIC ORDER BY count(Summons)ASC

# COMMAND ----------

# Create a view or table

temp_table_name = "Data"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  Sub_Division, count(Law_Section)   from `Data` group by Sub_Division 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Violation_County,   count(Summons)  from `Data` group by Violation_County
# MAGIC ORDER BY count(Summons)ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Sub_Division,  Violation_Description, Summons  from `Data`   group by Sub_Division

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select Dia, count(Summons) from `Data` group by Dia
# MAGIC ORDER BY Dia ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Mes,   count(Summons)  from `Data` group by Mes 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Street_Name,   count(Summons)  from `Data` group by Street_Name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select    count(Street_Name)  from `Data`  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Street_Name,   count(Summons)  from `Data` group by Street_Name

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "ParkingTotal2-1_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
