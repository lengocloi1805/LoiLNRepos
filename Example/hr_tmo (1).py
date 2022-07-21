# Databricks notebook source
# import basic library
import json
import re
import os
import pandas as pd
from pyspark.sql.types import *
from pyspark.pandas import read_excel
from spacex.synapse_utils.schema_data import schema_data
from spacex.utils import *

from spacex.synapse_utils.standard_punc import standard_punc
from spacex.synapse_utils.convert_vi2en import convert

# import spark session for convert spark to pandas
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print("start session")

# COMMAND ----------

# DBTITLE 1,Config Parameter
### Creating widgets for leveraging parameters, and printing the parameters, text() takes from 3 to 4 positional arguments
dbutils.widgets.text("db_save", "blob_fileName", "blob_folderPath")
                     
### get parameters    
db_save = dbutils.widgets.get("db_save")
blob_fileName = dbutils.widgets.get("blob_fileName")
blob_folderPath = dbutils.widgets.get("blob_folderPath")

# db_save = "tmo"
# blob_fileName = "New Joiner's Story_My 1st Working Day (J).xlsx"
# blob_folderPath = "tmo-hr"

partition = "Period"
write_mode = "merge"
primary_key = "Account"

if "1st Working Day" in blob_fileName:
    COL_NAMES = ["ID", "Start_time", "Completion_time", "Email", "Name", "Account", "OB_N1_emotion", "OB_N1_cmt"]
    table_name = "first_day_data"
    par_val = "N+1"
elif "1st Week" in blob_fileName:
    COL_NAMES = ['ID', 'Start_time', 'Completion_time', 'Email', 'Name', 'Account', 'OB_N7_connect_MNG', 'OB_N7_connect_team', \
                 'OB_N7_induction_act', 'OB_N7_interact_ex_elites', 'OB_N7_logistics_support', 'OB_N7_dissatisfied', 'OB_N7_problem', 'OB_N7_cmt']
    table_name = "first_week_data"
    par_val = "N+7"
elif "45 Days" in blob_fileName:
    if "(J)" in blob_fileName:
        COL_NAMES = ["ID", "Start_time", "Completion_time", "Email", "Name", "Account", "OB_N45_work_guideline", "OB_N45_MNG_guide_consult", \
                     "OB_N45_performance_act", "OB_N45_engagement_initiatives", "OB_N45_leadership_climate", "OB_N45_career_path", \
                     "OB_N45_cmt_career_path", "OB_N45_cmt"]
    elif "(S)" in blob_fileName:
        COL_NAMES = ["ID", "Start_time", "Completion_time", "Email", "Name", "Account", "OB_N45_work_guideline", "OB_N45_performance_act", \
                 "OB_N45_colleague_companionship", "OB_N45_engagement_initiatives", "OB_N45_assigned_task", "OB_N45_career_path", \
                 "OB_N45_cmt_career_path", "OB_N45_cmt"]
    else:
        COL_NAMES = ["ID", "Start_time", "Completion_time", "Email", "Name", "Account", "OB_N45_work_guideline", \
                 "OB_N45_engagement_initiatives", "OB_N45_assigned_task", "OB_N45_career_path", "OB_N45_cmt_career_path", \
                 "OB_N45_income_offer", "OB_N45_cmt"]
    table_name = "data_45th"
    par_val = "N+45"
    write_mode = "overwrite"
else:
    pass

# COMMAND ----------

# DBTITLE 1,Link Service
container = blob_folderPath.split("/")[0]
file_name = blob_fileName

print(f"container name: {container}\
       \nfile_name: {file_name}")

create_db_query = "CREATE DATABASE IF NOT EXISTS {}".format(db_save)
spark.sql(create_db_query)

accountname = "spacexdatalake"
apikey = "hpup2sap3NSHscnxS9gCALjic8VUCvMjIP5wfHTeyJavV4UKpuOz4hn7fPqjGAKWbVIHykGWX3VwfVhWGaJ50Q=="

### session configuration
spark.conf.set(f"fs.azure.account.key.{accountname}.dfs.core.windows.net", apikey)

# ### list all blobs in container
# blob_names = dbutils.fs.ls(f"abfss://{container}@{accountname}.dfs.core.windows.net/")
# display(blob_names)

# COMMAND ----------

# DBTITLE 1,Transform data
path_read = f"abfss://{container}@{accountname}.dfs.core.windows.net/{file_name}"
print(f"data read from: {path_read}")

pandas_df = read_excel(path_read, names=COL_NAMES)
pandas_df[primary_key]  = pandas_df[primary_key].apply(lambda row: row.split("@")[0])

pandas_df[partition] = [par_val]*len(pandas_df)

dict_df = pandas_df.to_dict()
new_pandas_df = pd.DataFrame(dict_df)

if table_name == "data_45th":
    if spark._jsparkSession.catalog().tableExists(db_save, table_name):
        base_df = spark.sql("SELECT * FROM {}.{}".format(db_save, table_name))
        base_df = base_df.toPandas()
        
        base_df = base_df.astype(str)
        new_pandas_df = new_pandas_df.astype(str)
        
        new_pandas_df = base_df.merge(new_pandas_df, on=[col for col in list(base_df.columns) if col in list(new_pandas_df.columns)], how="outer")
    else:
        pass

new_pandas_df.sort_values(by=["Start_time"], inplace=True)
new_pandas_df = new_pandas_df.drop_duplicates(subset=[primary_key], keep="last")

spark_df = spark.createDataFrame(new_pandas_df.astype(str))
display(spark_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Setting Schema for table
schema_config_raw = """
{
    "first_day_data":{
        "datetime": [],
        "float": [],
        "int": ["ID"],
        "boolean": [],
        "timestamp": ["Start_time", "Completion_time"]
        },
    "first_week_data":{
        "datetime": [],
        "float": [],
        "int": ["ID", "OB_N7_connect_MNG", "OB_N7_connect_team", "OB_N7_induction_act", "OB_N7_logistics_support"],
        "boolean": [],
        "timestamp": ["Start_time", "Completion_time"]
        },
    "data_45th":{
        "datetime": [],
        "float": [],
        "int": ["ID", "OB_N45_work_guideline", "OB_N45_MNG_guide_consult", "OB_N45_performance_act", "OB_N45_engagement_initiatives", \
                "OB_N45_leadership_climate", "OB_N45_career_path", "OB_N45_colleague_companionship", "OB_N45_assigned_task"],
        "boolean": [],
        "timestamp": ["Start_time", "Completion_time"]
    }
}

"""

schema_config = json.loads(schema_config_raw)
spark_df = schema_data(spark_df=spark_df, table_name=table_name, schema_config=schema_config)
spark_df = spark_df.replace('nan', None)

# COMMAND ----------

# DBTITLE 1,Save table to feature store with UPSERT
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

if table_name == "data_45th":
    fs.drop_table(name="{}.{}".format(db_save, table_name))

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_save))

if spark._jsparkSession.catalog().tableExists(db_save, table_name):
    print(f"data {write_mode} to: {db_save}.{table_name}")
    fs.write_table(
        name="{}.{}".format(db_save, table_name),
        df=spark_df,
        mode=write_mode,
    )
else:
    print(f"creating table: {db_save}.{table_name}")
    fs.create_table(
        name="{}.{}".format(db_save, table_name),
        primary_keys="Account",
        df=spark_df,
        partition_columns=partition,
        description="first week table information",
    )

# COMMAND ----------

# fs.drop_table(name="tmo.first_day_data")

# COMMAND ----------

# %sql
# DROP DATABASE IF EXISTS tmo CASCADE;
