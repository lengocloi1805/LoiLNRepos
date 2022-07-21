# Databricks notebook source
# DBTITLE 1,Import lib
# import basic library
import json
import re
import os
import pandas as pd
from pyspark.sql.types import *

# import module external
from spacex.synapse_utils.schema_data import schema_data
from spacex.utils import *

# import spark session for convert spark to pandas
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import warnings
warnings.filterwarnings("ignore")

print("start session")

# COMMAND ----------

# MAGIC %md
# MAGIC #### work with table query

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS db_spacex_trigger_latest_tmp_logging CASCADE;

# COMMAND ----------

df = spark.sql("SELECT * FROM fs_spacex_trigger.akajob")
display(df.limit(3))

# COMMAND ----------

# DBTITLE 1,Query data file path
# Load the data from the save location.
user_delta = spark.read.format("delta").load(save_path)

display(user_delta.limit(2))

# COMMAND ----------

# DBTITLE 1,Create table
table_name = 'top1000user'
# drop table if exist
display(spark.sql("DROP TABLE IF EXISTS " + table_name))

# create table from delta file
sql_query = "CREATE TABLE {} USING DELTA LOCATION '{}'".format(table_name, save_path)
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Query the table
display(spark.table(table_name).select('tenantid', 'agerangeid').orderBy('agerangeid', ascending = False))

# COMMAND ----------

# DBTITLE 1,Visualize data
df_user = spark.table(table_name)
display(df_user.select('gender').orderBy('gender', ascending = False).groupBy('gender').count())

# COMMAND ----------

# DBTITLE 1,Count row
user_delta.count()

# COMMAND ----------

# DBTITLE 1,Show partitions and contents of a partition
display(spark.sql("SHOW PARTITIONS " + table_name))

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/top1000_user/gender=0')

# COMMAND ----------

# DBTITLE 1,Optimize table 
display(spark.sql("OPTIMIZE " + table_name))

# COMMAND ----------

# DBTITLE 1,Show table history
display(spark.sql("DESCRIBE HISTORY " + table_name))

# COMMAND ----------

# DBTITLE 1,Show table details
display(spark.sql("DESCRIBE DETAIL " + table_name))

# COMMAND ----------

# DBTITLE 1,Show the table format
display(spark.sql("DESCRIBE FORMATTED " + table_name))

# COMMAND ----------

# DBTITLE 1,Clean up
# Delete the table.
spark.sql("DROP TABLE " + table_name)
# Delete the Delta files.
dbutils.fs.rm(save_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### method of dbutils.fs utilities

# COMMAND ----------

# List files of folder
for ele in dbutils.fs.ls("dbfs:/FileStore/tables"):
    print(ele.path)

# COMMAND ----------

# Create folder
dbutils.fs.mkdirs("dbfs:/FileStore/shared_uploads/test_mkdir1")

# COMMAND ----------

# Put data to file
dbutils.fs.put("dbfs:/FileStore/shared_uploads/test_mkdir/hello_db.txt", "Hello Databricks!", True)

# COMMAND ----------

# Move file to another folder
dbutils.fs.mv("dbfs:/FileStore/shared_uploads/test_mkdir/hello_db.txt", "dbfs:/FileStore/shared_uploads/test_mkdir1/hello_db.txt")

# COMMAND ----------

# Show data in file
dbutils.fs.head("dbfs:/FileStore/shared_uploads/test_mkdir1/hello_db.txt")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storageAccount), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storageAccount), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storageAccount), clientID)
spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storageAccount), clientSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storageAccount), oauth2Endpoint)

# COMMAND ----------

#Directly accessing the ADLS Gen-2 fileystem and reading data from specific folder 
df_direct = spark.read.format("csv").option("header",True).load(f"abfss://{containerAccount}@{storageAccount}.dfs.core.windows.net/top1000_user.csv")
display(df_direct.limit(5))

# COMMAND ----------

# DBTITLE 1,Listed CSV files in direct ADLSGen2Utop folder
display(dbutils.fs.ls(f"abfss://{containerAccount}@{storageAccount}.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### try feature_store

# COMMAND ----------

# DBTITLE 1,Creating link config to datalake ADLSGen2
accountname = "spacexdatalake"
apikey = "hpup2sap3NSHscnxS9gCALjic8VUCvMjIP5wfHTeyJavV4UKpuOz4hn7fPqjGAKWbVIHykGWX3VwfVhWGaJ50Q=="

# session configuration
spark.conf.set(f"fs.azure.account.key.{accountname}.dfs.core.windows.net", apikey)

# container = "spacex-normalized-data"
# folder_name = "jira"

# list all blobs in container
blob_names = dbutils.fs.ls(f"abfss://{container}@{accountname}.dfs.core.windows.net/{folder_name}")
display(blob_names)

# COMMAND ----------

# DBTITLE 1,Load from table and write to datalake
accountname = "spacexdatalake"
apikey = "hpup2sap3NSHscnxS9gCALjic8VUCvMjIP5wfHTeyJavV4UKpuOz4hn7fPqjGAKWbVIHykGWX3VwfVhWGaJ50Q=="
database_src = "db_spacex_for_hr"
datalake_des = "spacex-normalized-data-latest"

# session configuration
spark.conf.set(f"fs.azure.account.key.{accountname}.dfs.core.windows.net", apikey)

blob_names = ["akajob", "employee", "ipms", "leave_list", "leave_log", "myfpt_discipline", "myfpt_recognition", "salary_history", "transfer_history", "pulse_survey",
             "user_skip_survey", "che", "tms", "jira", "jiraallocation", "jira_project_list"]
for table_name in blob_names[8:9]:
    table_name_cleaned = table_name + "_cleaned"
    #table_name_duplicate_log = blob_name + "_duplicate_log"
    
    write_mode = "overwrite"
    if "jira" in table_name and "jira_project_list" not in table_name:
        partition = "evaluate_month"
    elif table_name.startswith("che") or table_name.startswith("tms"):
        partition = "year"
    else:
        partition = "fetch_date"

    query_read = "SELECT * FROM {}.{}".format(database_src, table_name_cleaned)
    print(f"data read from {query_read}")
    spark_df = spark.sql(query_read)
    
#     ### drop column
#     spark_df = spark_df.drop(*["fetch_date"])
    
    path_write = "abfss://{}@{}.dfs.core.windows.net/{}".format(datalake_des, accountname, table_name)
    print(f"data {write_mode} to {path_write}\n")
    spark_df.write.mode(write_mode).partitionBy(partition).csv(path_write, header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS db_spacex_cleaned_latest.che_duplicate_log;
# MAGIC -- DROP DATABASE IF EXISTS fs_spacex_da_check_duplicate_tmp CASCADE;

# COMMAND ----------

# DBTITLE 1,Load csv file from datalake to db table
database_name = "db_spacex_trigger"
create_db_query = "CREATE DATABASE IF NOT EXISTS {}".format(database_name)
spark.sql(create_db_query)

accountname = "spacexdatalake"
apikey = "hpup2sap3NSHscnxS9gCALjic8VUCvMjIP5wfHTeyJavV4UKpuOz4hn7fPqjGAKWbVIHykGWX3VwfVhWGaJ50Q=="
container = "spacex-normalized-data"

# session configuration
spark.conf.set(f"fs.azure.account.key.{accountname}.dfs.core.windows.net", apikey)

def load_csv2table(csv_filename, container, accountname, database_name):
    path_read = "abfss://{}@{}.dfs.core.windows.net/{}/*".format(container, accountname, csv_filename)
    print(f"path_read: {path_read}")
    spark_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", "\"""")
        .load(path_read))
    
    print(spark_df.rdd.getNumPartitions())
#     ### normalize column name
#     new_column_names = []
#     for col in spark_df.columns:
#         new_column_names.append(standard_punc(col))
#     spark_df = spark_df.toDF(*new_column_names)
    
    ### save to database
    table_name = csv_filename.split(".")[0] + "_test" 
    query_save = "{}.{}".format(database_name, table_name)
    print(f"query_save: {query_save}")
    print(spark_df.columns)
    spark_df.write.mode("overwrite").saveAsTable(query_save)
    
csv_filename = "jira.csv"
load_csv2table(csv_filename, container, accountname, database_name)

# COMMAND ----------

# DBTITLE 1,Change column type
# from pyspark.sql.types import DateType
# spark_df1 = spark_df.withColumn("hire_date", spark_df['hire_date'].cast(DateType())).withColumn("termination_date", spark_df['termination_date'].cast(DateType()))
# spark_df1.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize updated records

# COMMAND ----------

database = "db_spacex_trigger_latest_logging"
table_name = "employee_updated"
partition = "fetch_date"
par_val = "2022-07-01"

spark_df = spark.sql("SELECT * FROM {}.{} WHERE {} == '{}'".format(database, table_name, partition, par_val))
#spark_df = spark.sql("SELECT * FROM {}.{}".format(database, table_name))
print(spark_df.count())
display(spark_df)
#spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_tmp))
#write_table(spark_df=spark_df, database_name=database_tmp, table_name=table_name, partition=partition, write_mode="overwrite")
# write_table(spark_df=spark_df, database_name=database_tmp, table_name=table_name+"_cleaned", partition="evaluate_month", write_mode="overwrite")
# write_table(spark_df=spark_df, database_name="db_spacex_for_hr", table_name=table_name+"_cleaned", partition="evaluate_month", write_mode="overwrite")

# COMMAND ----------

pandas_df = spark_df.toPandas()
df.head()

# COMMAND ----------

updated_hash = pandas_df["fetch_date"].value_counts().to_dict()
updated_hash = dict(sorted(updated_hash.items()))
updated_hash

# COMMAND ----------

import matplotlib.pyplot as plt
# multiple line plots
plt.figure(figsize=(16,7))
plt.plot(list(updated_hash.keys()), list(updated_hash.values()), marker='o', color='green', linewidth=2, label="updated")

# show legend
plt.legend()
plt.title(table_name)
plt.show()

# COMMAND ----------

import ast
df = pandas_df[pandas_df["fetch_date"]=="2022-07-01"]
df["Diff_columns"] = df["Diff_columns"].apply(lambda row: ast.literal_eval(row))
df["result"] = df["Diff_columns"].apply(lambda row: list(row.keys()))
# df = df.apply(pd.Series)
df.head()

# COMMAND ----------

dif_cols = df["result"].to_list()
dif_cols_total = []
for lst in dif_cols:
    dif_cols_total.extend(lst)

# COMMAND ----------

hash = {}
for col in list(set(dif_cols_total)):
    hash[col] = dif_cols_total.count(col)
hash

# COMMAND ----------

plt.figure(figsize=(16,10))
x = list(hash.keys())
y = list(hash.values())
plt.barh(x, y)

for index, value in enumerate(y):
    plt.text(value, index, str(value))
plt.xlabel("updated values on employee table 2022-07-01")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fast Check

# COMMAND ----------

# DBTITLE 1,Creating link config to datalake ADLSGen2
accountname = "spacexdatalake"
apikey = "hpup2sap3NSHscnxS9gCALjic8VUCvMjIP5wfHTeyJavV4UKpuOz4hn7fPqjGAKWbVIHykGWX3VwfVhWGaJ50Q=="

# session configuration
spark.conf.set(f"fs.azure.account.key.{accountname}.dfs.core.windows.net", apikey)

container = "spacex-result"
# folder_name = "jira"

# list all blobs in container
blob_names = dbutils.fs.ls(f"abfss://{container}@{accountname}.dfs.core.windows.net")
display(blob_names)

# COMMAND ----------

spark_df = spark.sql("SELECT * FROM {}.{}".format("db_spacex_trigger_latest", "jira_project_list"))
display(spark_df)

# COMMAND ----------

pandas_df = spark_df.toPandas()
pandas_df[pandas_df["fsoftprojectid"]=="Body Shopping"]

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS db_spacex_cleaned_latest_tmp CASCADE;
# MAGIC DROP DATABASE IF EXISTS db_spacex_for_hr_tmp CASCADE;
# MAGIC DROP DATABASE IF EXISTS db_spacex_trigger_latest_tmp CASCADE;
# MAGIC DROP DATABASE IF EXISTS db_spacex_trigger_latest_logging_tmp CASCADE;

# COMMAND ----------


