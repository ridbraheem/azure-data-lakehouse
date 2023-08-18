# Databricks notebook source
# MAGIC %md
# MAGIC #### Create Final Table that will be sent to LLM Model
# MAGIC

# COMMAND ----------

# Read the data from the intermediate table
df_investments_funding = spark.read.table("dev_db.intermediate.investments_funding")
df_active_companies = spark.read.table("dev_db.intermediate.active_companies")

# COMMAND ----------

# Get the latest date for each org_uuid
df_latest_raise = (
    df_investments_funding.groupBy("org_uuid")
    .agg({"announced_on": "max"})
    .withColumnRenamed("MAX(announced_on)", "latest_raise")
)

# COMMAND ----------

# Get the amount raised, investments for latest raise for each org_uuid
import pyspark.sql.functions as F

df_latest_funding = (
    df_latest_raise.alias("a")
    .join(
        df_investments_funding.alias("b"),
        (F.col("a.org_uuid") == F.col("b.org_uuid"))
        & (F.col("a.latest_raise") == F.col("b.announced_on")),
        "left",
    )
    .groupBy("a.org_uuid", "a.latest_raise")
    .agg(
        F.sum(F.col("b.raised_amount_usd")).alias("raised_amount"),
        F.array_join(F.collect_set(F.col("b.investments")), ", ").alias("investments"),
    )
)

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit

# Join Active companies to latest funding to get each company's latest funding
df_active_companies_latest = df_active_companies.join(
    df_latest_funding, on="org_uuid", how="left"
).select(
    col("org_uuid"),
    col("name"),
    col("org_type"),
    col("country_code"),
    col("homepage_url"),
    col("status"),
    col("description"),
    col("investments"),
    concat_ws(
        " ",
        col("description"),
        lit(" Website: "),
        col("homepage_url"),
        lit(" Headquarter: "),
        col("country_code"),
        lit(" Recent Investments: "),
        col("investments"),
        lit(" Amount Raised: "),
        col("raised_amount"),
        lit(" Date Announced: "),
        col("latest_raise"),
    ).alias("company_information"),
)

# COMMAND ----------

# Write the table to Unity Catalog.
df_active_companies_latest.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_db.core.active_companies_information")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_db.core.active_companies_information
