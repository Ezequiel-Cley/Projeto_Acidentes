# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Dimenssão de Envolvidos

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamento do Dataframe para obter informações desejadas
# MAGIC
# MAGIC Aqui vamos filtrar apenas as colunas desejadas e realizar o tratamentod e tipagem e limpeza de dados necessárias
# MAGIC
# MAGIC Vamos realizar alguns tratamentos
# MAGIC
# MAGIC     Seleção de colunas necessárias
# MAGIC     Remoção de duplicidades aproveitado já o campo "pesid" que vem no arquivo original (Por ser o campo unico de cada envolvido)
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

Diretorio_Silver = '/Volumes/workspace/dw_acidentes/02_silver'

# COMMAND ----------

# Lê os arquivos Parquet da bronze
df_Acidentes_Causas = spark.read.parquet(Bronze_Acidentes_Causas)

# COMMAND ----------

# Selecionado apenas as colunas necessária para criar o modelo Dimenssional de Envolvidos
df_envolvidos = df_Acidentes_Causas.select(['pesid','tipo_envolvido', 'estado_fisico', 'idade', 'sexo'])

# COMMAND ----------

# Removendo Duplicadas df de veiculos no campo chave
df_envolvidos = df_envolvidos.dropDuplicates(subset=["pesid"])

# COMMAND ----------

# Renomeado a coluna para padrão desejado
df_envolvidos = df_envolvidos.withColumnRenamed('pesid', 'id_envolvido')

# COMMAND ----------

# REMOVER VALORES INVÁLIDOS ou nulos ANTES DO CAST(Alteração da Tipagem dos campos)
df_envolvidos = df_envolvidos.filter(
    (col("id_envolvido").isNotNull()) &
    (col("id_envolvido") != "NA")
)

# COMMAND ----------

# Configurado o nome das colunas para manter um padrão de minuscula e com "_" no lugar de espaçamentos
# Renomear e substituir espaços por underscore e todas as colunas convertendo para minúsculas
df_envolvidos = df_envolvidos.toDF(
    *[c.lower().replace(" ", "_") for c in df_envolvidos.columns]
)

# COMMAND ----------

# Alterando a tipagem para padrões necesários
df_envolvidos = df_envolvidos.withColumn('id_envolvido', col('id_envolvido').try_cast(IntegerType()))
df_envolvidos = df_envolvidos.withColumn('idade', col('idade').try_cast(IntegerType()))

# COMMAND ----------

# Retirando os nulos usando como parametro o campo chave pois não pode ser nulo
df_envolvidos = df_envolvidos.dropna(subset='id_envolvido')

# COMMAND ----------

# Validando os campos para realizar os tratamentos
df_envolvidos.printSchema()

# COMMAND ----------

# Avaliando os primeiros registros analise rapida.
display(df_envolvidos.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
df_envolvidos.write.parquet(
    f'{Diretorio_Silver}/Dim_Envolidos', 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)