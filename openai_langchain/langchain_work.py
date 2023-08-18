# Databricks notebook source
# MAGIC %md
# MAGIC ## Use Langchain and Open AI to interact with a Databricks Tables
# MAGIC

# COMMAND ----------


#import necessary libraries 

import os
from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain import OpenAI



os.environ["OPENAI_API_KEY"] =" "
 
#Connect to database and create agent

db = SQLDatabase.from_databricks(catalog="dev_db", schema="core")
llm = OpenAI(temperature=.7)
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
agent = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

# COMMAND ----------

#Ask Questions in Simple english and see how the agent converts it to sql to respond

agent.run("How many companies do we have with an ipo status")

# COMMAND ----------

#Read table into dataframe

df_companies_info = spark.read.table("dev_db.core.active_companies_information")

# COMMAND ----------

#Import necessary library

import openai
import tiktoken
from pyspark.ml.linalg import *
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from langchain.embeddings.openai import OpenAIEmbeddings

# COMMAND ----------

KEY=" "

# COMMAND ----------

df_companies_info.count()

# COMMAND ----------

#Create function to get number of tokens from 'company_information' column  

def num_tokens_from_string(string: str) -> int:
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    num_tokens = len(encoding.encode(string))
    return num_tokens

# COMMAND ----------

#Get tokens from 'company_information' column to see if there are values with large tokens

tokenUDF = udf(lambda x:num_tokens_from_string(x)) 
df_with_token = df_companies_info.withColumn("num_tokens", tokenUDF(col("company_information")))
df_with_token = df_with_token.withColumn("num_tokens",col("num_tokens").cast("int"))
df_with_token.select(col("company_information"),col("num_tokens")).show()

# COMMAND ----------

#Create Function to extract sectors using openAI and Langchain

from langchain import PromptTemplate

def extract_company_sectors(text):
    resume_template = """
    Assuming this is a brief description of a company. Extract key information and respond with an array of sectors the company is in. Do not output anything except for the extracted information. Do not add any clarifying information.All output must be in array format below 
    []

    {text}
    """

    prompt = PromptTemplate(
        input_variables=["text"],
        template=resume_template
    )

    summary_prompt = prompt.format(text=text)
    summary = llm(summary_prompt)

    return summary.strip()

# COMMAND ----------

# Register sectors function
sectorUDF = udf(extract_company_sectors, StringType())


#Get sectors and create column called "sectors"
df_with_sectors = df_with_token.withColumn(
    "sectors", sectorUDF(df_with_token["company_information"])
)

df_with_sectors = df_with_sectors.select(col("org_uuid"),col("name"), col("sectors")).show()

df_with_sectors.show()