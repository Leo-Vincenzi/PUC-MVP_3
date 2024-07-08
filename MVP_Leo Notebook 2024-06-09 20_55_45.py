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
# - criando container 2 - camada silver (tabelas limpas e preparadas para uso SQL)
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
# - criando container 3 - camada gold (visualizações, dashboards, agregações de tabelas)
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
import pandas as pd

# COMMAND ----------

# --------IPEA--------
#
# criando database "ipea"
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
# criando arquivo formato parquet na camada silver
df_ipea_silver.write.format('delta').mode('overwrite').save('dbfs:/mnt/mvpleo/silver/mvp3_ipea')
#
# usando database "ipea"---
spark.sql ('USE ipea')
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
# lendo as tabelas delta camada silver
df_ipea_silver = spark.read.format('delta').load('dbfs:/mnt/mvpleo/silver/mvp3_ipea')
#
# exportando database p/ "mvp3_idea.csv"
df_ipea_silver.write.csv('dbfs:/mnt/mvpleo/silver/mvp3_ipea.csv', header=True, mode="overwrite")
#
# fazendo uma query em mvp3_ipea para exame de conteúdo
mvp3_ipea = spark.sql("SELECT * FROM mvp3_ipea")
#
display(mvp3_ipea)

# COMMAND ----------

# --------TCE--------
#
# criando database "tce"
spark.sql ("CREATE DATABASE IF NOT EXISTS tce")
# lendo arquivo csv do Ipea na camada bronze
df_tce_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/mvp3_tce.csv')
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
# criando arquivo formato parquet na camada silver
df_tce_silver.write.format('delta').mode('overwrite').option('mergeSchema', 'true').option('overwriteSchema', 'true').save('dbfs:/mnt/mvpleo/silver/mvp3_tce')
#
# usando database "tce"---
spark.sql ('USE tce')
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
# lendo as tabelas delta camada silver
df_tce_silver = spark.read.format('delta').load('dbfs:/mnt/mvpleo/silver/mvp3_tce')
#
# exportando database p/ "mvp3_tce.csv"
df_tce_silver.write.csv('dbfs:/mnt/mvpleo/silver/mvp3_tce.csv', header=True, mode="overwrite")
#
# fazendo uma query em mvp3_tce para exame de conteúdo
mvp3_tce = spark.sql("SELECT * FROM mvp3_tce")
#
display(mvp3_tce)

# COMMAND ----------

# --------WIKI
#
# criando database "wiki"
spark.sql ("CREATE DATABASE IF NOT EXISTS wiki")
# lendo arquivo csv do Ipea na camada bronze
df_wiki_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/mvp3_wiki.csv')
#
# eliminando as colunas desnecessarias
df_wiki_silver = df_wiki_bronze.select('codigo_rj', 'wiki')
#
# limpando as linhas vazias das colunas selecionadas
df_wiki_silver = df_wiki_silver.filter(df_wiki_silver.codigo_rj.isNotNull())
df_wiki_silver = df_wiki_silver.filter(df_wiki_silver.wiki.isNotNull())
#
# convertendo campo "codigo_rj" de string p/ "integer"
df_wiki_silver = df_wiki_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
#
# criando arquivo formato parquet na camada silver
df_wiki_silver.write.format('delta').mode('overwrite').option('mergeSchema', 'true').option('overwriteSchema', 'true').save('dbfs:/mnt/mvpleo/silver/mvp3_wiki')
#
# usando database "wiki"---
spark.sql ('USE wiki')
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
# fazendo uma query
mvp3_wiki = spark.sql ('SELECT * FROM wiki.mvp3_wiki')
#
# lendo as tabelas delta camada silver
df_wiki_silver = spark.read.format('delta').load('dbfs:/mnt/mvpleo/silver/mvp3_wiki')
#
# fazendo uma query em mvp3_wiki para exame de conteúdo
mvp3_wiki = spark.sql("SELECT * FROM mvp3_wiki")
#
display(mvp3_wiki)

# COMMAND ----------

# --------IBGE--------
#
# criando database "ibge"
spark.sql ("CREATE DATABASE IF NOT EXISTS ibge")
# lendo arquivo csv do Ipea na camada bronze
df_ibge_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';'
    ).load('dbfs:/mnt/mvpleo/bronze/mvp3_ibge.csv')
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
# convertendo campo "codigo_rj" de string p/ "integer"
df_ibge_silver = df_ibge_silver.withColumn('codigo_rj', col('codigo_rj').cast('int'))
# convertendo campo "receita" de string p/ "float"
df_ibge_silver = df_ibge_silver.withColumn('area', regexp_replace(col('area'), ',', '.').cast('float'))
# convertendo campo "despesa" de string p/ "float"
df_ibge_silver = df_ibge_silver.withColumn('populacao', regexp_replace(col('populacao'), ',', '.').cast('int'))
# convertendo campo "receita" de string p/ "float"
df_ibge_silver = df_ibge_silver.withColumn('densidade', regexp_replace(col('densidade'), ',', '.').cast('float'))
# convertendo campo "despesa" de string p/ "float"
df_ibge_silver = df_ibge_silver.withColumn('escolaridade', regexp_replace(col('escolaridade'), ',', '.').cast('float'))
# convertendo campo "itai" de string p/ "float"
df_ibge_silver = df_ibge_silver.withColumn('pib', regexp_replace(col('pib'), ',', '.').cast('float'))
#
# criando arquivo formato Parquet na camada silver
df_ibge_silver.write.format('delta').mode('overwrite').option('mergeSchema', 'true').option('overwriteSchema', 'true').save('dbfs:/mnt/mvpleo/silver/mvp3_ibge')
#
# usando database "ibge"---
spark.sql ('USE ibge')
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
# lendo as tabelas delta camada silver
df_ibge_silver = spark.read.format('delta').load('dbfs:/mnt/mvpleo/silver/mvp3_ibge')
#
# exportando database p/ "mvp3_ibge.csv"
df_ipea_silver.write.csv('dbfs:/mnt/mvpleo/silver/mvp3_ibge.csv', header=True, mode="overwrite")
#
# carregando database "ibge"
spark.sql("USE ibge")
#
# fazendo uma query em mvp3_ibge para exame de conteúdo
mvp3_ibge = spark.sql("SELECT * FROM mvp3_ibge")
#
display(mvp3_ibge)

# COMMAND ----------

# fazendo uma query juntando todos os 4 dataframes em uma unica tabela 
spark.sql("USE CATALOG hive_metastore;")
spark.sql("USE SCHEMA ipea;")
spark.sql("DROP TABLE IF EXISTS dt_tabela;")
#
#    mvp3_tabela = spark.sql("""
#    SELECT DISTINCT ipea.*, ibge.*,tce.*,wiki.* 
#    FROM ipea.mvp3_ipea AS ipea 
#    INNER JOIN ibge.mvp3_ibge AS ibge ON (ipea.codigo_rj = ibge.codigo_rj)
#    INNER JOIN tce.mvp3_tce AS tce ON (ibge.codigo_rj = tce.codigo_rj)
#    INNER JOIN wiki.mvp3_wiki AS wiki ON (tce.codigo_rj = wiki.codigo_rj)
#    """)
#
mvp3_tabela = spark.sql("""
SELECT DISTINCT ipea.*,
ibge.codigo_rj AS ibge_codigo_rj, ibge.area AS area, ibge.populacao AS populacao, ibge.densidade AS densidade, ibge.escolaridade AS escolaridade,
tce.codigo_rj AS tce_codigo_rj, tce.receita AS receita, tce.despesa AS despesa, tce.itai AS itai,
wiki.codigo_rj AS wiki_codigo_rj, wiki.wiki AS wiki

FROM ipea.mvp3_ipea AS ipea 
INNER JOIN ibge.mvp3_ibge AS ibge ON (ipea.codigo_rj = ibge.codigo_rj)
INNER JOIN tce.mvp3_tce AS tce ON (ibge.codigo_rj = tce.codigo_rj)
INNER JOIN wiki.mvp3_wiki AS wiki ON (tce.codigo_rj = wiki.codigo_rj)
""")
#
# eliminando as colunas desnecessarias
mvp3_tabela = mvp3_tabela.select('codigo_rj', 'idhm', 'area', 'populacao', 'densidade', 'escolaridade', 'receita', 'despesa', 'itai', 'wiki')
#
# grava o dataframe para a tabela Delta
spark.sql("""DROP TABLE IF EXISTS mvp3_tabela""")
mvp3_tabela.write.format("delta").mode("overwrite").saveAsTable("tabela.mvp3_tabela")
spark.sql("USE SCHEMA tabela;")
#
# fazendo uma query em mvp3_tabela para exame de conteúdo
mvp3_tabela = spark.sql ('SELECT * FROM tabela.mvp3_tabela')
#
# mostra tabela agregada mvp3_tabela
display(mvp3_tabela)

# COMMAND ----------

# usando database "tabela"---
spark.sql("CREATE SCHEMA IF NOT EXISTS tabela")
spark.sql ('USE tabela')
#
# exportando "mvp3_tabela.csv" para camada gold
mvp3_tabela.write.csv('dbfs:/mnt/mvpleo/gold/mvp3_tabela', header=True, mode="overwrite")
#
# criando uma tabela externa na camanda gold
spark.sql("""
          CREATE OR REPLACE TABLE tabela.mvp3_tabela
          USING DELTA
          LOCATION '/mnt/mvpleo/gold/mvp3_tabela'
          """)
#
# fazendo uma query em mvp3_tabela
mvp3_tabela = spark.sql("SELECT * FROM tabela.mvp3_tabela")
#
display(mvp3_tabela)

# COMMAND ----------

# adicionando uma nova coluna data da carga
# from pyspark.sql.functions import current_date
# df_ipea_gold = df_ipea_silver.withColumn('dt_carga', current_date())

# COMMAND ----------

# adicionando a coluna 'year' e 'month'
# df_ipea_gold = df_ipea_gold.withColumn('year', year(df_ipea_gold['dt_carga']))
# df_ipea_gold = df_ipea_gold.withColumn('month', month(df_ipea_gold['dt_carga']))

# COMMAND ----------

# gravando os dados de volta no Data Lake, particionados por 'ano' e 'mes'
# df_ipea_gold.write.format('delta').mode('overwrite').option("mergeSchema", "true").partitionBy('YEAR', 'MONTH').save('/mnt/mvpleo/gold/mvp3_ipea_gold')

# COMMAND ----------

# dados de entrada
geojson_arquivo = "geojs_33_mun.json"
dados = np.genfromtxt("dados_calor_tce.csv", delimiter = ";")
