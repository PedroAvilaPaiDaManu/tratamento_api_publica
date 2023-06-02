# Databricks notebook source
pip install adal

# COMMAND ----------

pip install pandas

# COMMAND ----------

pip install requests

# COMMAND ----------

import adal
import pandas as pd
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

df = spark.read.option("multiline","true").json("caminhodoarquivo.json")

# COMMAND ----------

df.show(10)

# COMMAND ----------

df2 = df.select(col("id").alias("Id"),
               col("name").alias("Nome"),
               col("En_Nome_Logradouro").alias("Rua"),
               col("telefonePublico").alias("Telefone"),                
               col("En_Num").alias("Número"),
               col("En_Bairro").alias("Bairro"),
               col("En_Municipio").alias("Municipio"),
               col("En_Estado").alias("Estado"),
               col("En_CEP").alias("CEP"),
               col("acessibilidade").alias("acessibilidade"),
               col("public").alias("publico_ou_privado"),
               col("location.latitude").alias("Latitude"),
               col("shortDescription").alias("descrição_museu"),
               col("endereco").alias("endereco_museu"),
               col("location.longitude").alias("Longitude")
              )

# COMMAND ----------

#limpando a coluna telefone
df2 = df2.withColumn("Telefone", when(df2.Telefone == "", "Não Informado")
                                    .otherwise(df2.Telefone))

# COMMAND ----------

df2.show(10)

# COMMAND ----------

df2.select('publico_ou_privado').distinct().collect()

# COMMAND ----------

df2 = df2.withColumn("publico_ou_privado", when(df2.publico_ou_privado == "false", "Não").otherwise("Sim"))

# COMMAND ----------

df2.show(10)

# COMMAND ----------

df2.select('Estado').distinct().collect()

# COMMAND ----------

di_sg = {"Santa Catarina" :"SC","Alagoas":"AL","Rio Grande do Sul":"RS", "São Paulo": "SP","Rio de Janeiro":"RJ", "Maranhão" : "MA"}
for Estado in di_sg.keys():
    df2 = df2.withColumn("Estado", regexp_replace("Estado", Estado, di_sg[Estado]))

# COMMAND ----------

df2.select('Estado').distinct().collect()

# COMMAND ----------

from pyspark.sql.functions import col
df3 = df2.withColumn('regiao',
           when((col('Estado') == 'ES') | (col('Estado') == 'MG') | (col('Estado') == 'RJ') | (col('Estado') == 'SP'), 'Sudeste')
          .when((col('Estado') == 'AC') | (col('Estado') == 'PA') | (col('Estado') == 'RO') | (col('Estado') == 'RR') |
                (col('Estado') == 'TO') | (col('Estado') == 'AM') | (col('Estado') == 'AP') ,'Norte')
          .when((col('Estado') == 'RS') | (col('Estado') == 'SC') | (col('Estado') == 'PR') ,'Sul')
          .when((col('Estado') == 'GO') | (col('Estado') == 'MS') | (col('Estado') == 'MT') ,'Centro Oeste')
          .when((col('Estado') == 'MA') | (col('Estado') == 'PB') | (col('Estado') == 'PE') | (col('Estado') == 'PI') | 
                (col('Estado') == 'RN') | (col('Estado') == 'SE') | (col('Estado') == 'AL')| (col('Estado') == 'BA') | (col('Estado') == 'CE')
                 ,'Nordeste')                                          
          .otherwise('Estado'))

# COMMAND ----------

df3.show(10)

# COMMAND ----------

df3.select('publico_ou_privado').distinct().collect()

# COMMAND ----------

df_vazios = df3.select([count(when(col(c).isNull() , c)).alias(c) for c in df2.columns])
#display(df_vazios)

# COMMAND ----------

import adal
resource_app_id_url = "https://database.windows.net/"
service_principal_id = dbutils.secrets.get(scope = "scope", key = "app-reg-adb")
service_principal_secret = dbutils.secrets.get(scope = "scope", key = "app-user-databricks")
tenant_id = "codigo"
authority = "https://login.windows.net/" + tenant_id
azure_sql_url = "jdbc:sqlserver://sql-databasepedro.database.windows.net"
database_name = "db-estudos"
db_table = "pedro_avila.museus"
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]

# COMMAND ----------

df3.write \
.format("jdbc")\
.mode("overwrite")\
.partitionBy("Estado")\
.option("url", azure_sql_url) \
.option("dbtable", db_table) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df3.select('Telefone').distinct())
