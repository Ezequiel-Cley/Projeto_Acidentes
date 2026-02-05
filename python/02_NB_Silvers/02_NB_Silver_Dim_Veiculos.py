# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Dimenssão de Veiculos

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tratamento do Dataframe para obter informações desejadas
# MAGIC
# MAGIC Aqui vamos filtrar apenas as colunas desejadas e realizar o tratamentod e tipagem e limpeza de dados necessárias
# MAGIC
# MAGIC Vamos realizar alguns tratamentos
# MAGIC
# MAGIC     Seleção de colunas necessárias
# MAGIC     Remoção de duplicidades aproveitado já o ID_Veiculo que vem no arquivo original (Por ser o campo unico de cada veiculo já)
# MAGIC     Configurar colunas para o padrão
# MAGIC     Filtrando dados indesejados
# MAGIC     Alteração de tipagem de dados
# MAGIC     Removendo dados nulos
# MAGIC     Salvando arquivo de forma compactada

# COMMAND ----------

from pyspark.sql.types import * # Todos as configurações do SQL de tipagem no spark
from pyspark.sql.functions import * # Todas as funções de SQL do Spark
from pyspark.sql.window import Window

# COMMAND ----------

# Diretorio Bronze, com os dados brutos das causas dos acidentes
Bronze_Acidentes_Causas = '/Volumes/workspace/dw_acidentes/01_bronze/Acidentes_Causas/'

Diretorio_Silver = '/Volumes/workspace/dw_acidentes/02_Silver'

# COMMAND ----------

# Lê os arquivos Parquet da Bronze
df_Acidentes_Causas = spark.read.parquet(Bronze_Acidentes_Causas)

# COMMAND ----------

# Selecionado apenas as colunas necessária para criar o modelo Dimenssional de Veiculos
df_veiculos = df_Acidentes_Causas.select(['id_veiculo','tipo_veiculo', 'marca', 'ano_fabricacao_veiculo'])

# COMMAND ----------

# Removendo Duplicadas df de veiculos no campo chave
df_veiculos = df_veiculos.dropDuplicates(subset=["id_veiculo"])

# COMMAND ----------

# Configurado o nome das colunas para manter um padrão de minuscula e com underscore no lugar de espaçamentos
# Renomear todas as colunas convertendo para minúsculas
df_veiculos = df_veiculos.toDF(*[c.lower() for c in df_veiculos.columns])
# Renomear e substituir espaços por underscore
df_veiculos = df_veiculos.toDF(*[c.replace(' ', '_') for c in df_veiculos.columns])

# COMMAND ----------

# Removendo dados Indesejados dos do campo ID_Veiculos
df_veiculos = df_veiculos.where(col('id_Veiculo') != 'NA')

# COMMAND ----------

# Realizando transformações de tipagem no arquivo
df_veiculos = df_veiculos.withColumn('id_veiculo', col('id_veiculo').try_cast(IntegerType()))
df_veiculos = df_veiculos.withColumn('ano_fabricacao_veiculo', col('ano_fabricacao_veiculo').try_cast(IntegerType()))

# COMMAND ----------

# Retirando os nulos usando como parametro o campo chave pois não pode ser nulo
df_veiculos = df_veiculos.dropna(subset='id_veiculo')

# COMMAND ----------

# Validando os campos após os tratamentos realizados
df_veiculos.printSchema()

# COMMAND ----------

# Avaliando os primeiros registros analise rapida.
display(df_veiculos.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip
df_veiculos.write.parquet(
    f"{Diretorio_Silver}/Dim_Veiculos", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)