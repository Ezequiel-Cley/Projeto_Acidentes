# Databricks notebook source
# MAGIC %md
# MAGIC - ## Leitura das tabelas Silver para salvar como Tabelas Deltas

# COMMAND ----------

# MAGIC %md
# MAGIC Processo consiste em ler todos os arquivos da parquet para criar arquivos versionados em delta parquet, na tabela gold e gerar as tabelas para uso em Consultas SQL, para analista de Dados e BI.

# COMMAND ----------

# Importando Bibliotecas para uso do Spark
from pyspark.sql import SparkSession # Import para inciar o spark
from pyspark.sql.types import * # Todos as configurações do SQL de tipagem no spark
from pyspark.sql.functions import * # Todas as funções de SQL do Spark
from pyspark.sql.window import Window

# COMMAND ----------

# Local para armazenar as tabelas Gold
diretorio_gold = '/Volumes/workspace/dw_acidentes/03_gold'

# Local para armazenar as tabelas Silvers
diretorio_Silver = '/Volumes/workspace/dw_acidentes/02_silver'

#Dimenssões necessárias para os relacionamentos
localizacao = fr'{diretorio_Silver}/Dim_Localizacao'
clima = fr'{diretorio_Silver}/Dim_Clima'
calenadario = fr'{diretorio_Silver}/Dim_Calendario'
envolvidos = fr'{diretorio_Silver}/Dim_Envolidos'
veiculos = fr'{diretorio_Silver}/Dim_Veiculos'
acidentes = fr'{diretorio_Silver}/Facts_Acidentes'

# COMMAND ----------

# Iniciando leitura de todos as tabelas
dim_localizacao = spark.read.parquet(localizacao)
dim_clima = spark.read.parquet(clima)
dim_calenadario = spark.read.parquet(calenadario)
dim_envolvidos = spark.read.parquet(envolvidos)
dim_veiculos = spark.read.parquet(veiculos)
fact_acidentes = spark.read.parquet(acidentes)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando a Tabela Localização

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
dim_localizacao.write.parquet(
    f"{diretorio_gold}/dim_localizacao", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

dim_localizacao.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.dim_localizacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvado a tabela Calendario

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
dim_calenadario.write.parquet(
    f"{diretorio_gold}/dim_calendario", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

dim_calenadario.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.dim_calenadario")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando a tabela envolvidos

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
dim_envolvidos.write.parquet(
    f"{diretorio_gold}/dim_envolvidos", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

dim_envolvidos.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.dim_envolvidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando a tabela Clima

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
dim_clima.write.parquet(
    f"{diretorio_gold}/dim_clima", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

dim_clima.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.dim_clima")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando a tabela Veiculo

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
dim_veiculos.write.parquet(
    f"{diretorio_gold}/dim_veiculos", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

dim_veiculos.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.dim_veiculos")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando a tabela Fato de Acidentes

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
fact_acidentes.write.parquet(
    f"{diretorio_gold}/fact_acidentes", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

fact_acidentes.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.dw_acidentes.fact_acidentes")