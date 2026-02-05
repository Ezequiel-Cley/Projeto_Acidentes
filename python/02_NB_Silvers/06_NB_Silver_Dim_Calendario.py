# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Dimenssão de Calendario

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
df_calendario = df_Acidentes_Causas.select(['data_inversa', 'dia_semana'])

# COMMAND ----------

# Removendo Duplicadas para criar o campo chave
df_calendario = df_calendario.dropDuplicates()

# COMMAND ----------

# Criando a função de janela e ordenando todos as colunas aplicando linha a linha 
window = Window.orderBy('data_inversa', 'dia_semana')

# Aplicando a função row_number e over para cada linha incluir o ID incremental.
df_calendario = df_calendario.withColumn(
    "id_calendario",
    row_number().over(window)
)

# COMMAND ----------

# Enriquecendo adicionado informações como trimestre, mês e dia
df_calendario = (
    df_calendario
    .withColumn("trimestre", quarter("data_inversa"))
    .withColumn("mes", month("data_inversa"))
    .withColumn("dia", dayofmonth("data_inversa"))
)

# COMMAND ----------

# Enriquecedo realizado calculo para se obter a semana do mês na tabela
df_calendario = df_calendario.withColumn(
    "semana_do_mes",
    floor(
        (
            dayofmonth("data_inversa")
            + dayofweek(date_trunc("month", "data_inversa"))
            - 1
        ) / 7
    ) + 1
)

# COMMAND ----------

# Enriquecedo realizado calculo para se obter a semana do ano na tabela
df_calendario = df_calendario.withColumn("semana_do_ano", weekofyear("data_inversa"))

# COMMAND ----------

# Realizado condicional para se obter o nome do mês em Portugês na tabela
df_calendario = df_calendario.withColumn(
    "nome_do_mes",
    when(month("data_inversa") == 1, "Janeiro")
    .when(month("data_inversa") == 2, "Fevereiro")
    .when(month("data_inversa") == 3, "Março")
    .when(month("data_inversa") == 4, "Abril")
    .when(month("data_inversa") == 5, "Maio")
    .when(month("data_inversa") == 6, "Junho")
    .when(month("data_inversa") == 7, "Julho")
    .when(month("data_inversa") == 8, "Agosto")
    .when(month("data_inversa") == 9, "Setembro")
    .when(month("data_inversa") == 10, "Outubro")
    .when(month("data_inversa") == 11, "Novembro")
    .otherwise("Dezembro")
)

# COMMAND ----------

# Renomeado a coluna para padrão desejado
df_calendario = df_calendario.withColumnRenamed('data_inversa', 'data_acidente')

# COMMAND ----------

# Definido a posição das colunas conforme documetnação
df_calendario = df_calendario.select(['id_calendario', 'data_acidente', 'dia_semana', 'trimestre', 'mes', 'dia', 'semana_do_mes', 'semana_do_ano', 'nome_do_mes'])

# COMMAND ----------

# Avaliando se o schema atende a documentação
df_calendario.printSchema()

# COMMAND ----------

# Avaliando os primeiros Registros
display(df_calendario.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
df_calendario.write.parquet(
    f"{Diretorio_Silver}/Dim_Calendario", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)