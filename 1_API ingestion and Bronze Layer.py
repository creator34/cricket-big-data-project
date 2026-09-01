# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import the required libraries
#we do not need to define a session/context as databricks is built on top of session
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *  

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")
base_path="/Volumes/workspace/default/cricket_api_project"

# COMMAND ----------

# DBTITLE 1,Calling cricket API
API_KEY="2a8f9667-b1e1-446b-92ea-75a7ece5b170"
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response=requests.get(api_url)
response.raise_for_status()

api_data=response.json()

# COMMAND ----------

# DBTITLE 1,Save raw api response in the volumes
raw_file_path=f'{base_path}/current_matches_raw.json'
with open(raw_file_path,'w') as file:
    json.dump(api_data,file)



# COMMAND ----------

# DBTITLE 1,Create the bronze layer table or df
bronze_data=[{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

bronze_schema=StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("ingestion_time",TimestampType(),True)
])

bronze_df=(
    spark
    .createDataFrame(bronze_data,schema=bronze_schema)
    .withColumn("ingestion_time",current_timestamp())
    )


# COMMAND ----------

# DBTITLE 1,Save the bronze table
(
    bronze_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable("workspace.default.cricket_bronze_current_matches")
)

# COMMAND ----------

