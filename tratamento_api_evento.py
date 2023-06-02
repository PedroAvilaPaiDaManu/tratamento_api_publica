# Databricks notebook source
pip install adal

# COMMAND ----------

pip install pandas

# COMMAND ----------

from pyspark.sql.types import*
import pandas as pd
from pyspark.sql.functions import*
from pyspark.sql import functions as f
import adal


# COMMAND ----------

pip install requests

# COMMAND ----------

#extraindo a api do blob
dados_json = spark.read.format("json").option("inferSchema", "true").load("wasbs://caminhodoarquivo.json")

df = spark.read.option("multiline","true").json("wasbs://caminhodoarquivo.json")





# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

df.show(10)

# COMMAND ----------

#selecionado as colunas que vou utilizar e importando as Bibliotecas
import requests, json
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col ,split, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DataType, IntegerType
df2 = df.select(col("occurrences.space.id").alias("space_id"),
                col("name").alias("Titulo_do_evento"),
                col("occurrences.rule.description").alias("Descrição_do_evento"),
                col("occurrences.rule.duration").alias("Duração_do_evento"),
                col("occurrences.rule.startsOn").alias("Inicio_do_evendo"),
                col("occurrences.rule.until").alias("Final_do_evento"),
                col("occurrences.rule.frequency").alias("Frequencia"),
                col("occurrences.rule.price").alias("Preço"),
                col("terms.linguagem").alias("Linguagem"),
                col("occurrences.space.singleUrl").alias("site"))

# COMMAND ----------

#comando para tirar do array
from pyspark.sql.functions import *
df3 = df2 \
    .withColumn("space_id",concat_ws(", ",col("space_id")))\
    .withColumn("Titulo_do_evento",concat_ws(", ",col("Titulo_do_evento")))\
    .withColumn("Descrição_do_evento",concat_ws(", ",col("Descrição_do_evento")))\
    .withColumn("Duração_do_evento",concat_ws(", ",col("Duração_do_evento")))\
    .withColumn("Inicio_do_evendo",concat_ws(", ",col("Inicio_do_evendo")))\
    .withColumn("Final_do_evento",concat_ws(", ",col("Final_do_evento")))\
    .withColumn("Frequencia",concat_ws(", ",col("Frequencia")))\
    .withColumn("Preço",concat_ws(", ",col("Preço")))\
    .withColumn("Linguagem",concat_ws(", ",col("Linguagem")))\
    .withColumn("site",concat_ws(", ",col("site")))



# COMMAND ----------

df3.show(10)

# COMMAND ----------

#copia o df
df4 = df3.select("*")

# COMMAND ----------

df4 = df4.withColumn("Fim_do_evento", substring("Final_do_evento", 1,10))

# COMMAND ----------

df4.select('space_id').distinct().collect()

# COMMAND ----------

df5 = df4.withColumn("Fim_do_evento", when(df4.Fim_do_evento == ", 2019-08-", "Não Informado")
                                      .when(df4.Fim_do_evento == "', '", "Não Informado")
                                      .when(df4.Fim_do_evento == "", "Não Informado")
                                      .when(df4.Fim_do_evento == ".", "Não Informado")
                                      .when(df4.Fim_do_evento == "''", "Não Informado")
                                      .when(df4.Fim_do_evento == ", , , ", "Não Informado")
                                      .when(df4.Fim_do_evento == ", ", "Não Informado")
                                      .otherwise(df4.Fim_do_evento))

# COMMAND ----------

df5.show(10)

# COMMAND ----------

#comando para pegar da tabela inicio_do_evento as 10 primeiras casas
df5 = df5.withColumn("Data", substring("Inicio_do_evendo", 1,10))


# COMMAND ----------

#  df4 = df4.withColumn("Inicio_do_evendo", when(df4.Inicio_do_evendo == "2019-07-05, 2019-07-05", "2019-07-05")
#                                               .when(df4.Inicio_do_evendo == "2019-10-08, 2019-10-08", "2019-10-08")
#                                               .when(df4.Inicio_do_evendo == "2019-10-14, 2019-10-14", "2019-10-14")
#                                               .when(df4.Inicio_do_evendo == "2019-07-06, 2019-07-06", "2019-07-06")
#                                               .when(df4.Inicio_do_evendo == "2019-12-09, 2019-12-10, 2019-12-11, 2019-12-12", "2019-12-09")
#                                               .when(df4.Inicio_do_evendo == "2021-07-12, 2021-07-12", "2021-07-12")
#                                              .otherwise(df4.Inicio_do_evendo))

# COMMAND ----------

df5 = df5.drop('Inicio_do_evendo')

# COMMAND ----------

df5.show(10)

# COMMAND ----------

df5 = df5.withColumn("Fim_do_evento", when(df5.Fim_do_evento == ", 2019-08-", "Não Informado")
                                      .when(df5.Fim_do_evento == "', '", "Não Informado")
                                      .when(df5.Fim_do_evento == "", "Não Informado")
                                      .when(df5.Fim_do_evento == ".", "Não Informado")
                                      .when(df5.Fim_do_evento == "''", "Não Informado")
                                      .when(df5.Fim_do_evento == ", , , ", "Não Informado")
                                      .when(df5.Fim_do_evento == ", ", "Não Informado")
                                      .otherwise(df5.Fim_do_evento))

# COMMAND ----------

from pyspark.sql.functions import *
df6 = (df5.withColumn("Data",to_date("Data", "yyyy-MM-dd")))\
            

# COMMAND ----------

df6.show(10)

# COMMAND ----------

df6.printSchema()

# COMMAND ----------

df6.select('space_id').distinct().collect()

# COMMAND ----------

df6 = df6.withColumn("space_id", when(df6.space_id == "8218, 8218, 8218, 8218", "8218")
                                 .when(df6.space_id == "6095, 6095", "6095")
                                 .when(df6.space_id == "6090, 6090", "6090")
                                 .when(df6.space_id == "6091, 6091", "6091")
                                 .when(df6.space_id == "6103, 6103", "6103")
                                 .when(df6.space_id == "6644, 6644", "6644")
                                 .when(df6.space_id == "6102, 6102", "6102")
                                  .otherwise(df6.space_id))

# COMMAND ----------

df6 = df6.withColumn("space_id",df6["space_id"].cast('integer'))
   

# COMMAND ----------

df6.show(10)

# COMMAND ----------

df6.filter(df6["Linguagem"] == '').count()

# COMMAND ----------

df6.show(10)

# COMMAND ----------

#arrumando coluna preço, Gratuito e Pago.
df6 = df6.withColumn("Preço", when(df6.Preço == "", "Gratuito")
                                 .when(df6.Preço == "gratuito", "Gratuito")
                                 .when(df6.Preço == "gratuito ", "Gratuito")
                                 .when(df6.Preço == "gratuita", "Gratuito")
                                 .when(df6.Preço == "Gratuita", "Gratuito")
                                 .when(df6.Preço == "grátis", "Gratuito")
                                 .when(df6.Preço == "Grátis", "Gratuito")
                                 .when(df6.Preço == "GRATUITO", "Gratuito")         
                                 .when(df6.Preço == ",", "Gratuito")
                                 .when(df6.Preço == "0", "Gratuito")
                                 .when(df6.Preço == "0,00", "Gratuito")
                                 .when(df6.Preço == "entrada gratuita", "Gratuito")
                                 .when(df6.Preço == "Entrada gratuita", "Gratuito")
                                 .when(df6.Preço == "Entrada Franca", "Gratuito")
                                 .when(df6.Preço == "entrada franca", "Gratuito")
                                 .when(df6.Preço == "Entrada Franca ", "Gratuito")
                                 .when(df6.Preço == "Entrada Franca, Entrada Franca, Entrada Franca, Entrada Franca", "Gratuito")
                                 .when(df6.Preço == "gratuito, gratuito", "Gratuito")
                                 .when(df6.Preço == ", ", "Gratuito")
                                 .when(df6.Preço == "Gratuito ", "Gratuito")
                                 .when(df6.Preço == "Gratuito", "Gratuito")
                                 .when(df6.Preço == ", ", "Gratuito")
                                 .otherwise("Pago"))

# COMMAND ----------

#limpando coluna frequancia
df6 = df6.withColumn("Frequencia", when(df6.Frequencia == "daily", "Diário")
                                   .when(df6.Frequencia == "once", "Semanal")
                                   .when(df6.Frequencia == "weekly", "Semanal")
                                   .when(df6.Frequencia == "once, weekly", "Semanal")
                                   .when(df6.Frequencia == "once, once, once, once", "Semanal")
                                   .when(df6.Frequencia == "once, once", "Semanal")
                                   .when(df6.Frequencia == "daily, daily", "Diário")
                                  .otherwise(df6.Frequencia))

# COMMAND ----------

df6.show(10)

# COMMAND ----------

df6 = df6.drop('Final_do_evento')
   

# COMMAND ----------

df6 = df6.drop('Duração_do_evento')

# COMMAND ----------

#display(df6.select('Frequencia').distinct())

# COMMAND ----------

display(df6)

# COMMAND ----------

import adal
resource_app_id_url = "https://database.windows.net/"
service_principal_id = dbutils.secrets.get(scope = "scope", key = "app-reg-adb")
service_principal_secret = dbutils.secrets.get(scope = "scope-", key = "app-user-databricks")
tenant_id = "chave"
authority = "https://login.windows.net/" + tenant_id
azure_sql_url = "jdbc:sqlserver://sql-estudo.database.windows.net"
database_name = "db-estudos"
db_table = "pedro_avila.db_STAGE_eventos_lab_10"
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]

# COMMAND ----------

df6.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", db_table) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()
