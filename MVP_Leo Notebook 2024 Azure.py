# Databricks notebook source
# ETL usando arquitetura em camadas (p/ extração, transformação e carga) - MVP3 Leonardo Braga De Vincenzi - PUC-Rio
#
import os
#
# - criando container 1 - camada bronze (dado bruto, sem transformação)
#
mount_point = "/mnt/mvpleo/bronze"
already_mounted = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())

if already_mounted:
    print("A camada bronze já existe!")
else:
    dbutils.fs.mount(
    source = "wasbs://bronze@mvpleo.blob.core.windows.net",
    extra_configs = {"fs.azure.account.key.mvpleo.blob.core.windows.net":
                    "H0QHQKK17VJoCe5tcS8ei6NkwXSpqg5p88deIrT1/LFuHz/gr1+Kui/awrFCTFt4nQrYEairIO5z+AStCd2nsA=="}
    )
# - criando container 2 - camada silver (tabelas limpas e preparadas para uso do SQL)
#
mount_point = "/mnt/mvpleo/silver"
already_mounted = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())

if already_mounted:
        print("A camada silver já existe!")
else:
    dbutils.fs.mount(
    source = "wasbs://silver@mvpleo.blob.core.windows.net",
    extra_configs = {"fs.azure.account.key.mvpleo.blob.core.windows.net":
                    "H0QHQKK17VJoCe5tcS8ei6NkwXSpqg5p88deIrT1/LFuHz/gr1+Kui/awrFCTFt4nQrYEairIO5z+AStCd2nsA=="}
    )
# - criando container 3 - camada gold (visualizações, dashboards, e agregações de tabelas)
#
mount_point = "/mnt/mvpleo/gold"
already_mounted = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())

if already_mounted:
        print("A camada gold já existe!")
else:
    dbutils.fs.mount(
    source = "wasbs://gold@mvpleo.blob.core.windows.net",
    extra_configs = {"fs.azure.account.key.mvpleo.blob.core.windows.net":
                      "H0QHQKK17VJoCe5tcS8ei6NkwXSpqg5p88deIrT1/LFuHz/gr1+Kui/awrFCTFt4nQrYEairIO5z+AStCd2nsA=="}
    )

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col, to_date, year, month, regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import pandas as pd

# COMMAND ----------

# --------Municipio--------
#
# criando database "municipio"
spark.sql ("DROP DATABASE IF EXISTS municipio CASCADE")
spark.sql ("CREATE DATABASE IF NOT EXISTS municipio")
# lendo arquivo csv do municipio na camada bronze
df_municipio_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';', encoding='ISO-8859-1'
    ).load('dbfs:/mnt/mvpleo/bronze/municipio.csv')
#
# eliminando as colunas desnecessárias
df_municipio_silver = df_municipio_bronze.select('codigo_rj', 'nome_municipio', 'regiao', 'latitude', 'longitude')
#
# limpando as linhas vazias das colunas selecionadas
df_municipio_silver = df_municipio_silver.filter(df_municipio_silver.codigo_rj.isNotNull())
df_municipio_silver = df_municipio_silver.filter(df_municipio_silver.nome_municipio.isNotNull())
df_municipio_silver = df_municipio_silver.filter(df_municipio_silver.regiao.isNotNull())
df_municipio_silver = df_municipio_silver.filter(df_municipio_silver.latitude.isNotNull())
df_municipio_silver = df_municipio_silver.filter(df_municipio_silver.longitude.isNotNull())
#
# convertendo campo "codigo_rj" de string p/ "integer"
df_municipio_silver = df_municipio_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
# convertendo campo "latitude" de string p/ "float"
df_municipio_silver = df_municipio_silver.withColumn('latitude', regexp_replace(col('latitude'), ',', '.').cast('float'))
# convertendo campo "longitude" de string p/ "float"
df_municipio_silver = df_municipio_silver.withColumn('longitude', regexp_replace(col('longitude'), ',', '.').cast('float'))
#
# gravando arquivo formato Parquet (delta) na camada silver
df_municipio_silver.write.format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save('/mnt/mvpleo/silver/mvp3_municipio')
#
# usando database "municipio"---
spark.sql ('USE SCHEMA municipio')
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_municipio
          """)
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_municipio
          USING DELTA
          LOCATION '/mnt/mvpleo/silver/mvp3_municipio'
          """)
#
# fazendo uma query em mvp3_ipea para exame de conteúdo
mvp3_municipio = spark.sql("SELECT * FROM mvp3_municipio")
#
display(mvp3_municipio)

# COMMAND ----------

# --------IPEA--------
#
# criando database "ipea"
spark.sql ("DROP DATABASE IF EXISTS ipea CASCADE")
spark.sql ("CREATE DATABASE IF NOT EXISTS ipea")
# lendo arquivo csv do Ipea na camada bronze
df_ipea_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/RJ_Mun97_region.csv')
#
# eliminando as colunas desnecessárias
df_ipea_silver = df_ipea_bronze.select('Codigo do Municipio', 'IDHM')
#
# renomeando colunas
df_ipea_silver = df_ipea_silver.withColumnRenamed('Codigo do Municipio','codigo_rj').withColumnRenamed('IDHM','idhm')
#
# limpando as linhas vazias das colunas selecionadas
df_ipea_silver = df_ipea_silver.filter(df_ipea_silver.codigo_rj.isNotNull())
df_ipea_silver = df_ipea_silver.filter(df_ipea_silver.idhm.isNotNull())
#
# convertendo campo "codigo_rj" de string p/ "integer"
df_ipea_silver = df_ipea_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
# convertendo campo "idhm" de string p/ "float"
df_ipea_silver = df_ipea_silver.withColumn('idhm', regexp_replace(col('idhm'), ',', '.').cast('float'))
#
# gravando arquivo formato Parquet (delta) na camada silver
df_ipea_silver.write.format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save('/mnt/mvpleo/silver/mvp3_ipea')
#
# usando database "ipea"---
spark.sql ('USE SCHEMA ipea')
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_ipea
          """)
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_ipea
          USING DELTA
          LOCATION '/mnt/mvpleo/silver/mvp3_ipea'
          """)
#
# fazendo uma query em mvp3_ipea para exame de conteúdo
mvp3_ipea = spark.sql("SELECT * FROM mvp3_ipea")
#
display(mvp3_ipea)

# COMMAND ----------

# --------TCE--------
#
# criando database "tce"
spark.sql ("DROP DATABASE IF EXISTS tce CASCADE")
spark.sql ("CREATE DATABASE IF NOT EXISTS tce")
# lendo arquivo csv do Ipea na camada bronze
df_tce_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/tce.csv')
#
# eliminando as colunas desnecessárias
df_tce_silver = df_tce_bronze.select('codigo_rj', 'receita', 'despesa', 'itai')
#
# limpando as linhas vazias das colunas selecionadas
df_tce_silver = df_tce_silver.filter(df_tce_silver.codigo_rj.isNotNull())
df_tce_silver = df_tce_silver.filter(df_tce_silver.receita.isNotNull())
df_tce_silver = df_tce_silver.filter(df_tce_silver.despesa.isNotNull())
df_tce_silver = df_tce_silver.filter(df_tce_silver.itai.isNotNull())
#
# convertendo campo "codigo_rj" de string p/ "integer"
df_tce_silver = df_tce_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
# convertendo campo "receita" de string p/ "float"
df_tce_silver = df_tce_silver.withColumn('receita', regexp_replace(col('receita'), ',', '.').cast('float'))
# convertendo campo "despesa" de string p/ "float"
df_tce_silver = df_tce_silver.withColumn('despesa', regexp_replace(col('despesa'), ',', '.').cast('float'))
# convertendo campo "itai" de string p/ "float"
df_tce_silver = df_tce_silver.withColumn('itai', regexp_replace(col('itai'), ',', '.').cast('float'))
#
# gravando arquivo formato Parquet (delta) na camada silver
df_tce_silver.write.format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save('/mnt/mvpleo/silver/mvp3_tce')
#
# usando database "tce"---
spark.sql ('USE SCHEMA tce')
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_tce
          """)
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_tce
          USING DELTA
          LOCATION '/mnt/mvpleo/silver/mvp3_tce'
          """)
#
# fazendo uma query em mvp3_tce para exame de conteúdo
mvp3_tce = spark.sql("SELECT * FROM mvp3_tce")
#
display(mvp3_tce)

# COMMAND ----------

# --------WIKI
#
# criando database "wiki"
spark.sql ("DROP DATABASE IF EXISTS wiki CASCADE")
spark.sql ("CREATE DATABASE IF NOT EXISTS wiki")
# lendo arquivo csv do Ipea na camada bronze
df_wiki_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/wiki.csv')
#
# eliminando as colunas desnecessarias
df_wiki_silver = df_wiki_bronze.select('codigo_rj', 'link_wiki')
#
# limpando as linhas vazias das colunas selecionadas
df_wiki_silver = df_wiki_silver.filter(df_wiki_silver.codigo_rj.isNotNull())
df_wiki_silver = df_wiki_silver.filter(df_wiki_silver.link_wiki.isNotNull())
#
# convertendo campo "codigo_rj" de string p/ "integer"
df_wiki_silver = df_wiki_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
#
# gravando arquivo formato Parquet (delta) na camada silver
df_wiki_silver.write.format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save('/mnt/mvpleo/silver/mvp3_wiki')
#
# usando database "wiki"---
spark.sql ('USE SCHEMA wiki')
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_wiki
          """)
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_wiki
          USING DELTA
          LOCATION '/mnt/mvpleo/silver/mvp3_wiki'
          """)
#
# fazendo uma query em mvp3_wiki para exame de conteúdo
mvp3_wiki = spark.sql("SELECT * FROM mvp3_wiki")
#
display(mvp3_wiki)

# COMMAND ----------

# --------IBGE--------
#
# criando database "ibge"
spark.sql ("DROP DATABASE IF EXISTS ibge CASCADE")
spark.sql ("CREATE DATABASE IF NOT EXISTS ibge")
#
# Definindo schema de "ibge"
ibge_schema = StructType([
   StructField('codigo_rj', IntegerType()),
   StructField('area', FloatType()),
   StructField('populacao', IntegerType()),
   StructField('densidade', FloatType()),
   StructField('escolaridade', FloatType()),
   StructField('pib', FloatType()),                  
])
# lendo arquivo csv do Ipea na camada bronze
df_ibge_bronze = spark.read.format('csv') \
    .option('header', 'true') \
    .option('delimiter', ';') \
    .schema(ibge_schema) \
    .load('dbfs:/mnt/mvpleo/bronze/ibge.csv')
#
# eliminando as colunas desnecessarias
df_ibge_silver = df_ibge_bronze.select('codigo_rj', 'area', 'populacao', 'densidade', 'escolaridade', 'pib')
#
# limpando as linhas vazias das colunas selecionadas
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.codigo_rj.isNotNull())
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.area.isNotNull())
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.populacao.isNotNull())
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.densidade.isNotNull())
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.escolaridade.isNotNull())
df_ibge_silver = df_ibge_silver.filter(df_ibge_silver.pib.isNotNull())
#
# criando arquivo formato Parquet na camada silver
df_ibge_silver.write.format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save('/mnt/mvpleo/silver/mvp3_ibge')
#
# usando database "ibge"---
spark.sql ('USE SCHEMA ibge')
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_ibge""")
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_ibge
          USING DELTA
          LOCATION '/mnt/mvpleo/silver/mvp3_ibge'
          """)
#
# fazendo uma query em mvp3_ibge para exame de conteúdo
mvp3_ibge = spark.sql("SELECT * FROM mvp3_ibge")
#
display(mvp3_ibge)

# COMMAND ----------

# fazendo uma query juntando todos as 5 tabelas em 1 unica tabela 
spark.sql("USE CATALOG hive_metastore;")
spark.sql("USE SCHEMA ipea;")
spark.sql("CREATE SCHEMA IF NOT EXISTS tabelao")
spark.sql("DROP TABLE IF EXISTS dt_tabelao;")
#
mvp3_tabelao = spark.sql("""
SELECT DISTINCT ipea.*,
ibge.codigo_rj AS ibge_codigo_rj, ibge.area AS area, ibge.populacao AS populacao, ibge.densidade AS densidade, ibge.escolaridade AS escolaridade,
tce.codigo_rj AS tce_codigo_rj, tce.receita AS receita, tce.despesa AS despesa, tce.itai AS itai,
wiki.codigo_rj AS wiki_codigo_rj, wiki.link_wiki AS link_wiki,
municipio.codigo_rj AS municipio_codigo_rj, municipio.nome_municipio AS nome_municipio, municipio.regiao AS regiao, municipio.latitude AS latitude, municipio.longitude AS longitude

FROM ipea.mvp3_ipea AS ipea 
INNER JOIN ibge.mvp3_ibge AS ibge ON (ipea.codigo_rj = ibge.codigo_rj)
INNER JOIN tce.mvp3_tce AS tce ON (ibge.codigo_rj = tce.codigo_rj)
INNER JOIN wiki.mvp3_wiki AS wiki ON (tce.codigo_rj = wiki.codigo_rj)
INNER JOIN municipio.mvp3_municipio AS municipio ON (wiki.codigo_rj = municipio.codigo_rj)
""")
#
# eliminando as colunas desnecessarias da tabela agregada
mvp3_tabelao = mvp3_tabelao.select(
    'codigo_rj', 'idhm', 'area', 'populacao', 'densidade', 'escolaridade', 'receita', 'despesa', 'itai', 'link_wiki', 'nome_municipio', 'regiao', 'latitude', 'longitude'
)
#
spark.sql("USE SCHEMA tabelao;")
# gravando a tabela agregada no formato Parquet (delta) na camada "gold"
mvp3_tabelao.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("header", "true").save('/mnt/mvpleo/gold/mvp3_tabelao')

# exportando a tabela agregada "mvp3_tabelao" no formato "CSV" para a camada "gold"
# mvp3_tabelao.write.format("csv").mode("overwrite").option("overwriteSchema", "true").option("header", "true").save('/mnt/mvpleo/gold/mvp3_tabelao.csv')

# usando database "tabelao"---
spark.sql("USE SCHEMA tabelao;")
#
# criando tabela externa
spark.sql("""
          DROP TABLE IF EXISTS mvp3_tabelao
          """)
#
spark.sql("""
          CREATE TABLE IF NOT EXISTS mvp3_tabelao
          USING DELTA
          LOCATION '/mnt/mvpleo/gold/mvp3_tabelao'
          """)
#
# fazendo uma query em mvp3_tabelao para exame de conteúdo
mvp3_tabelao = spark.sql("SELECT * FROM mvp3_tabelao")
#
# mostra tabela agregada mvp3_tabelao
display(mvp3_tabelao)

# COMMAND ----------

!pip install pandablob -q
import pandas as pd
import numpy as np
import pandablob
import io
from datetime import datetime
from azure.storage.blob import ContainerClient, BlobClient

# COMMAND ----------

# exportando a tabela agregada "mvp3_tabelao" no formato "CSV" para a camada "gold"
#
# Lendo Parquet no Spark dataFrame
df = (spark.read
  .format("delta")
  .option("mode", "PERMISSIVE")
  .option("header", "true")
  .load("/mnt/mvpleo/gold/mvp3_tabelao")
)
# convertendo em Pandas dataframe
pandas_df = df.toPandas()

# Detalhando o Azure Blob Storage 
account_url = 'https://mvpleo.blob.core.windows.net'
token = 'H0QHQKK17VJoCe5tcS8ei6NkwXSpqg5p88deIrT1/LFuHz/gr1+Kui/awrFCTFt4nQrYEairIO5z+AStCd2nsA=='
container = 'gold'
blobname = "mvp3_tabelao.csv"

container_client = ContainerClient(account_url=account_url, container_name=container, credential=token)
blob_client = container_client.get_blob_client(blob=blobname)

# Convertendo Pandas dataframe em CSV and salvando no Azure Blob Storage
blob_client.upload_blob(pandas_df.to_csv(index=False), overwrite=True)

# COMMAND ----------

# Quais são os municípios que possuem idhm e itai maiores que 0.75 ?
#
spark.sql("USE SCHEMA tabelao;")
SQL1 = spark.sql("""
                  SELECT nome_municipio 
                  FROM mvp3_tabelao
                  WHERE idhm > 0.75 AND itai > 0.8
                  """)
display(SQL1)

# COMMAND ----------

# Quais são os 5 municípios que possuem maior receita ?
#
spark.sql("USE SCHEMA tabelao;")
SQL2 = spark.sql("""
                  SELECT nome_municipio FROM mvp3_tabelao
                  ORDER BY receita DESC LIMIT 5
                  """)
display(SQL2)

# COMMAND ----------

# Quais são os 6 municípios que possuem maior populacao ? Mostre as suas quantidade de habitantes.
#
spark.sql("USE SCHEMA tabelao;")
SQL3 = spark.sql("""
                  SELECT nome_municipio, populacao FROM mvp3_tabelao
                  ORDER BY populacao DESC LIMIT 6
                  """)
display(SQL3)

# COMMAND ----------

# Quais são os municípios que possuem receita maior que despesa e idhm alto ou muito alto (entre 0.750 e 0.849)?
#
spark.sql("USE SCHEMA tabelao;")
SQL4 = spark.sql("""
                  SELECT nome_municipio
                  FROM mvp3_tabelao
                  WHERE receita > despesa AND idhm > 0.75
                  """)
display(SQL4)

# COMMAND ----------

# Quais são os percentuais totais dos municípios por regiao ?
#
spark.sql("USE SCHEMA tabelao;")
SQL5 = spark.sql("""
                  SELECT regiao,
                         CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(10,2)) AS percentage
                  FROM mvp3_tabelao
                  GROUP BY regiao
                  ORDER BY percentage DESC 
                  LIMIT 10
                  """)
display(SQL5)

# COMMAND ----------



# COMMAND ----------

!curl ipecho.net/plain
