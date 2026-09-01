# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,importing libraries
from pyspark.sql.functions import *


# COMMAND ----------

# DBTITLE 1,read the silver layer
silver_df=spark.table('workspace.default.cricket_silver_current_matches' )


# COMMAND ----------

# DBTITLE 1,gold analytics 1 : match type distribution
gold_match_type_df=silver_df.groupBy('match_type').agg(count('*').alias('total_matches'))
display(gold_match_type_df)

# COMMAND ----------

# DBTITLE 1,gold analytics 2 : venue wise match count
gold_venue_df=silver_df.groupBy('venue').agg(count('*').alias('total_matches'))
display(gold_venue_df)

# COMMAND ----------

# DBTITLE 1,gold analytics 3 : team wise match count
team1_df=silver_df.select(col('team_1').alias('team'))
team2_df=silver_df.select(col('team_2').alias('team'))

all_teams_df=team1_df.union(team2_df)

gold_team_df=all_teams_df.groupBy('team').agg(count('*').alias('matches_played'))
display(gold_team_df)


# COMMAND ----------

# DBTITLE 1,Final Analytics queries
#match overview
display(spark.sql("""select
count(*) as total_matches,
count(distinct match_type) as total_match_types,
count(distinct venue) as total_venues
from workspace.default.cricket_silver_current_matches"""))




# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC count(*) as total_matches,
# MAGIC count(distinct match_type) as total_match_types,
# MAGIC count(distinct venue) as total_venues
# MAGIC from workspace.default.cricket_silver_current_matches

# COMMAND ----------

