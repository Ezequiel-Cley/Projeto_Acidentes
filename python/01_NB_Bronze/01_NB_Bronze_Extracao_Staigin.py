# Databricks notebook source
# MAGIC %md
# MAGIC ## Leitura dos arquivos na Staigin
# MAGIC
# MAGIC Aqui é a etapa de descompactação dos arquivos zip e leitura dos mesmo para o tratamento e criação do Armazenamento Bronze
# MAGIC
# MAGIC Link de acesso aos dados:
# MAGIC https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf

# COMMAND ----------

import zipfile # Biblioteca para extração de arquivos em zip

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definições de Variaveis

# COMMAND ----------

# --------------------------------------------------------------------- #
# ----- Arquivos de nível acidentes todas as causas encontradas ------- #
# --------------------------------------------------------------------- #
# Caminho do arquivo com todas as informações e cuasas do acidentes.
causas_zip = '/Volumes/workspace/projetoacidentes/03_acidentes_por_causa'
# Caminho temporario para armazenar os arquivos descompactados
diretorio_Staigin_causas = '/Volumes/workspace/dw_acidentes/00_staigin/Acidentes_Causas'

# --------------------------------------------------------------------- #
# ---------- Arquivos de nível acidentes causa principal -------------- #
# --------------------------------------------------------------------- #
# Caminho dos arquivos no nível mais macro apenas acidentes
acidentes_zip = '/Volumes/workspace/projetoacidentes/01_acidentes'
# Caminho temporario para armazenar os arquivos descompactados
diretorio_Staigin_acidentes = '/Volumes/workspace/dw_acidentes/00_staigin/Acidentes'

# --------------------------------------------------------------------- #
# ---------- Local para armaazenar os arquivos bronze ----------------- #
# --------------------------------------------------------------------- #
diretorio_bronze = '/Volumes/workspace/dw_acidentes/01_bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizado descompactação de todos os arquivos e Leitura dos arquivos Acidentes Causas

# COMMAND ----------

# 1. (Python) Descompacta o arquivo no diretório temporário
# Obtendo os dados dos arquivos para extração do zip
lista_arquivos = dbutils.fs.ls(causas_zip) 
lista_arquivos = [f.path for f in lista_arquivos]
lista_arquivos = [f.replace('dbfs:/', '/') for f in lista_arquivos] # Criando uma lista com o nome de todos os arquivos que estão na pasta

# Loop para interar sobre cada arquivo
for i in lista_arquivos:
    #caminho_completo_zip = paths_completos # Configurado o caminho completo ao qual deve ser descompactado 
    with zipfile.ZipFile(i, 'r') as zip_ref:
        zip_ref.extractall(diretorio_Staigin_causas) #Define o local onde o arquivo deve ser alocado após extração

# COMMAND ----------

# 2. (PySpark) Lê os arquivos CSV descompactado
df_Acidentes_Causas = spark.read.csv(diretorio_Staigin_causas, sep=';', header=True, inferSchema=True, encoding="ISO-8859-1")

# COMMAND ----------

# Visualizado os 5 primeiros registros para fins de entender o arquivo
display(df_Acidentes_Causas.limit(5))

# COMMAND ----------

# Salvado o Dataframe em tabela bronze
df_Acidentes_Causas.write.parquet(
    f'{diretorio_bronze}/Acidentes_Causas',
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizado descompactação de todos os arquivos e Leitura dos arquivos Acidentes

# COMMAND ----------

# 1. (Python) Descompacta o arquivo no diretório temporário
# Obtendo os dados dos arquivos para extração do zip
lista_arquivos = dbutils.fs.ls(acidentes_zip) 
lista_arquivos = [f.path for f in lista_arquivos]
lista_arquivos = [f.replace('dbfs:/', '/') for f in lista_arquivos] # Criando uma lista com o nome de todos os arquivos que estão na pasta

# Loop para interar sobre cada arquivo
for i in lista_arquivos:
    with zipfile.ZipFile(i, 'r') as zip_ref:
        zip_ref.extractall(diretorio_Staigin_acidentes) #Define o local onde o arquivo deve ser alocado após extração

# COMMAND ----------

# 2. (PySpark) Lê os arquivos CSV descompactado
df_Acidentes = spark.read.csv(diretorio_Staigin_acidentes, sep=';', header=True, inferSchema=True, encoding="ISO-8859-1")

# COMMAND ----------

# Visualizado os 5 primeiros registros para fins de entender o arquivo
display(df_Acidentes.limit(5))

# COMMAND ----------

# Salvado o Dataframe em tabela bronze
df_Acidentes.write.parquet(
    f'{diretorio_bronze}/Acidentes',
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)