# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Dimenssão de Localização

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamento do Dataframe para obter informações desejadas
# MAGIC
# MAGIC Aqui vamos filtrar apenas as colunas desejadas e realizar o tratamentod e tipagem e limpeza de dados necessárias
# MAGIC
# MAGIC Vamos realizar alguns tratamentos
# MAGIC
# MAGIC     Seleção de colunas necessárias
# MAGIC     Configurar colunas para o padrão
# MAGIC     Filtrando dados indesejados
# MAGIC     Alteração de tipagem de dados
# MAGIC     Removendo dados nulos
# MAGIC     Salvando arquivo de forma compactada

# COMMAND ----------

# Importando Bibliotecas para uso do Spark
from pyspark.sql import SparkSession # Import para inciar o spark
from pyspark.sql.types import * # Todos as configurações do SQL de tipagem no spark
from pyspark.sql.functions import * # Todas as funções de SQL do Spark
from pyspark.sql.window import Window

# COMMAND ----------

# Diretorio Bronze, com os dados brutos das causas dos acidentes
Bronze_Acidentes_Causas = '/Volumes/workspace/dw_acidentes/01_bronze/Acidentes_Causas/'

# Local para armazenar dimenssão
Diretorio_Silver = '/Volumes/workspace/dw_acidentes/02_silver'


# COMMAND ----------

# Lê os arquivos CSV descompactado
df_Acidentes_Causas = spark.read.parquet(Bronze_Acidentes_Causas)

# COMMAND ----------

# Selecionado apenas as colunas necessária para criar o modelo Dimenssional de Localização
df_localizacao = df_Acidentes_Causas.select(['municipio', 'uf', 'br', 'tipo_pista', 'km', 'tracado_via', 'uso_solo', 'sentido_via', 'regional', 'delegacia', 'uop', 'latitude', 'longitude'])

# COMMAND ----------

# Removendo Duplicadas para criar o campo chave
df_localizacao = df_localizacao.dropDuplicates()

# COMMAND ----------

# Criando a função de janela e ordenando todos as colunas aplicando linha a linha 
window = Window.orderBy('municipio', 'uf', 'br', 'tipo_pista', 'km', 'tracado_via', 'uso_solo', 'sentido_via', 'regional', 'delegacia', 'uop', 'latitude', 'longitude')

# Aplicando a função row_number e over para cada linha incluir o ID incremental.
df_localizacao = df_localizacao.withColumn(
    "id_localizacao",
    row_number().over(window)
)

# COMMAND ----------

# Selecionado apenas as colunas na ordem desejada
df_localizacao = df_localizacao.select(['id_localizacao', 'municipio', 'uf', 'br', 'tipo_pista', 'km', 'tracado_via', 'uso_solo', 'sentido_via', 'regional', 'delegacia', 'uop', 'latitude', 'longitude'])

# COMMAND ----------

# Alterando a tipagem para padrões necesários - Realizado "regexp_replace" para remover dados com "."" E substituir as "," por ".", por fim alterar os tipos para decimal
df_localizacao = df_localizacao.withColumn("km",regexp_replace(regexp_replace(col("km"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))
df_localizacao = df_localizacao.withColumn("latitude",regexp_replace(regexp_replace(col("latitude"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))
df_localizacao = df_localizacao.withColumn("longitude",regexp_replace(regexp_replace(col("longitude"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))

# COMMAND ----------

# Validando os campos para realizar os tratamentos
df_localizacao.printSchema()

# COMMAND ----------

# Visualizando o primeiros registros para visualizar o resultado final
display(df_localizacao.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
df_localizacao.write.parquet(
    f"{Diretorio_Silver}/Dim_Localizacao", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)