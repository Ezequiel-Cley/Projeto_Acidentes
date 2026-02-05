# Databricks notebook source
# MAGIC %md
# MAGIC ## Tabela Silver para Fatos de Acidentes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento do Dataframe para obter informações desejadas
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

# Importando Bibliotecas para uso do Spark
from pyspark.sql import SparkSession # Import para inciar o spark
from pyspark.sql.types import * # Todos as configurações do SQL de tipagem no spark
from pyspark.sql.functions import * # Todas as funções de SQL do Spark
from pyspark.sql.window import Window

# COMMAND ----------

# Diretorio Bronze, com os dados brutos das causas dos acidentes
Bronze_Acidentes = '/Volumes/workspace/dw_acidentes/01_bronze/Acidentes/'
Bronze_Acidentes_Causas = '/Volumes/workspace/dw_acidentes/01_bronze/Acidentes_Causas/'

# Local para armazenar as tabelas Silvers
diretorio_Silver = '/Volumes/workspace/dw_acidentes/02_silver'

#Dimenssões necessárias para os relacionamentos
localizacao = fr'{diretorio_Silver}/Dim_Localizacao'
clima = fr'{diretorio_Silver}/Dim_Clima'
calenadario = fr'{diretorio_Silver}/Dim_Calendario'

# COMMAND ----------

# Lê os arquivos CSV descompactado
df_Acidentes = spark.read.parquet(Bronze_Acidentes)
df_Acidentes_causas = spark.read.parquet(Bronze_Acidentes_Causas)

# COMMAND ----------

# Realizado leitura dos arquivos dimenssionais
dim_calenadario = spark.read.parquet(calenadario)
dim_clima = spark.read.parquet(clima)
dim_localizacao = spark.read.parquet(localizacao)

# COMMAND ----------

# Reduzindo o volume de dados para perfomance em relacionamento
dim_calenadario = dim_calenadario.select(['id_calendario','data_acidente'])

# COMMAND ----------

# Renomeado e alterando o tipo da coluna para padrão desejado para relacionamento
df_Acidentes_causas = df_Acidentes_causas.withColumnRenamed('data_inversa', 'data_acidente')
df_Acidentes_causas = df_Acidentes_causas.withColumn('data_acidente', to_date(col('data_acidente'), 'yyyy-MM-dd'))

# COMMAND ----------

# Realizando o relacionamento para onbter o dado do ID_Calendario da Dimenssão para tabela fato
df_Acidentes_causas = df_Acidentes_causas.join(dim_calenadario, on='data_acidente', how="left")

# COMMAND ----------

# Realizando o relacionamento para onbter o dado do ID_CLIME da Dimenssão para tabela fato 
df_Acidentes_causas = df_Acidentes_causas.join(dim_clima, on=['fase_dia', 'condicao_metereologica'], how="left")

# COMMAND ----------

# Alterando a tipagem para padrões necesários - Realizado "regexp_replace" para remover dados com "."" E substituir as "," por ".", por fim alterar os tipos para decimal
df_Acidentes_causas = df_Acidentes_causas.withColumn("km",regexp_replace(regexp_replace(col("km"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))
df_Acidentes_causas = df_Acidentes_causas.withColumn("latitude",regexp_replace(regexp_replace(col("latitude"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))
df_Acidentes_causas = df_Acidentes_causas.withColumn("longitude",regexp_replace(regexp_replace(col("longitude"), "\\.", ""), ",", ".").try_cast(DecimalType(18, 8)))

# COMMAND ----------

ids_para_relacao = ['municipio', 'uf', 'br', 'tipo_pista', 'km', 'tracado_via', 'uso_solo', 'sentido_via', 'regional', 'delegacia', 'uop', 'latitude', 'longitude']
# Realizando o relacionamento para onbter o dado do ID_CLIME da Dimenssão para tabela fato 
df_Acidentes_causas = df_Acidentes_causas.join(dim_localizacao, on=ids_para_relacao, how="left")

# COMMAND ----------

df_ponte = df_Acidentes_causas.select(['id', 'id_veiculo','pesid', 'id_calendario', 'id_clima', 'id_localizacao'])
df_ponte = df_ponte.dropDuplicates(subset=["id"])
df_Acidentes = df_Acidentes.join(df_ponte, on="id", how="left")

# COMMAND ----------

# Selecionado apenas as colunas necessária para criar o modelo Dimenssional de Veiculos
df_Facts_Acidentes = df_Acidentes.select(['id', 'id_localizacao', 'id_veiculo', 'id_clima', 
                                          'pesid', 'id_calendario', 'data_inversa', 'horario',
                                          'causa_acidente', 'tipo_acidente', 'classificacao_acidente',
                                          'pessoas', 'veiculos', 'ilesos',
                                          'feridos', 'feridos_leves', 'feridos_graves',
                                          'mortos', 'ignorados']
                                        )

# COMMAND ----------

# Removendo Duplicadas df de veiculos no campo chave
df_Facts_Acidentes = df_Facts_Acidentes.dropDuplicates(subset=["id"])

# COMMAND ----------

# Configurado o nome das colunas para manter um padrão de minuscula e com underscore no lugar de espaçamentos
# Renomear todas as colunas convertendo para minúsculas
df_Facts_Acidentes = df_Facts_Acidentes.toDF(*[c.lower() for c in df_Facts_Acidentes.columns])
# Renomear e substituir espaços por underscore
df_Facts_Acidentes = df_Facts_Acidentes.toDF(*[c.replace(' ', '_') for c in df_Facts_Acidentes.columns])

# COMMAND ----------

# Alterando a tipagem dos campos desejados
df_Facts_Acidentes = (
    df_Facts_Acidentes
    .withColumn('id', col('id').try_cast(IntegerType()))
    .withColumn('id_localizacao', col('id_localizacao').try_cast(IntegerType()))
    .withColumn('id_veiculo', col('id_veiculo').try_cast(IntegerType()))
    .withColumn('id_clima', col('id_clima').try_cast(IntegerType()))
    .withColumn('pesid', col('pesid').try_cast(IntegerType()))
    .withColumn('id_calendario', col('id_calendario').try_cast(IntegerType()))
    .withColumn('data_inversa', to_date(col('data_inversa'), 'yyyy-MM-dd'))
    .withColumn('horario',col('horario').try_cast("string"))
    .withColumn('pessoas',col('pessoas').cast(IntegerType()))
    .withColumn('veiculos',col('veiculos').cast(IntegerType()))
    .withColumn('ilesos',col('ilesos').cast(IntegerType()))
    .withColumn('feridos',col('feridos').cast(IntegerType()))
    .withColumn('feridos_leves',col('feridos_leves').cast(IntegerType()))
    .withColumn('feridos_graves',col('feridos_graves').cast(IntegerType()))
    .withColumn('mortos',col('mortos').cast(IntegerType()))
    .withColumn('ignorados',col('ignorados').cast(IntegerType()))
)

# COMMAND ----------

# Renomeado a coluna para padrão desejado
df_Facts_Acidentes = df_Facts_Acidentes.withColumnRenamed('id', 'id_acidente')
df_Facts_Acidentes = df_Facts_Acidentes.withColumnRenamed('data_inversa', 'data_acidente')
df_Facts_Acidentes = df_Facts_Acidentes.withColumnRenamed('horario', 'hora_acidente')
df_Facts_Acidentes = df_Facts_Acidentes.withColumnRenamed('pesid', 'id_envolvido')

# COMMAND ----------

# Validando os campos para realizar os tratamentos
df_Facts_Acidentes.printSchema()

# COMMAND ----------

# Avaliando os primeiros registros analise rapida.
display(df_Facts_Acidentes.limit(5))

# COMMAND ----------

# Salvando o arquivo em parquet com compressão gzip 
df_Facts_Acidentes.write.parquet(
    f"{diretorio_Silver}/Facts_Acidentes", 
    mode="overwrite",         # Aqui defino para sempre sobrescrever o arquivo
    compression="gzip"        # Comando para salvar o arquivo com compactação
)