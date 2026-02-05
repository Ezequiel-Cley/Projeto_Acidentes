# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Dimenssão de Clima

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

# Selecionado apenas as colunas necessária para criar o modelo Dimenssional de Clima
df_clima = df_Acidentes_Causas.select(['fase_dia', 'condicao_metereologica'])

# COMMAND ----------

# Removendo Duplicadas para criar o campo chave
df_clima = df_clima.dropDuplicates() 

# COMMAND ----------

# Criando a função de janela e ordenando todos as colunas aplicando linha a linha 
window = Window.orderBy('fase_dia', 'condicao_metereologica')

# Aplicando a função row_number e over para cada linha incluir o ID incremental.
df_clima = df_clima.withColumn(
    "id_clima",
    row_number().over(window)
)

# COMMAND ----------

# Definido a ordem conforme a documentação desejada
df_clima = df_clima.select(['id_clima', 'fase_dia', 'condicao_metereologica'])

# COMMAND ----------

# Validando os campos para realizar os tratamentos
df_clima.printSchema()

# COMMAND ----------

# Visualizando o primeiros registros para visualizar o resultado final 
display(df_clima.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
df_clima.write.parquet(
    f'{Diretorio_Silver}/Dim_Clima', 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)