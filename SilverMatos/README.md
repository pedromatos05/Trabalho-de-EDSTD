# Trabalho-de-EDSTD


``` python

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, trim, to_date, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


warehouse_location = "hdfs://hdfs-nn:9000/warehouse"


spark = (
    SparkSession.builder
    .appName("RottenTomatoes ETL")
    .config("spark.sql.warehouse.dir", warehouse_location)
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)


titles_schema = StructType([
    StructField("movie_title", StringType(), True),
    StructField("movie_info", StringType(), True),
    StructField("critics_consensus", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("in_theaters_date", StringType(), True), # Lemos como string
    StructField("on_streaming_date", StringType(), True), # Lemos como string
    StructField("runtime_in_minutes", IntegerType(), True),
    StructField("studio_name", StringType(), True),
    StructField("tomatometer_status", StringType(), True),
    StructField("tomatometer_rating", DoubleType(), True),
    StructField("tomatometer_count", DoubleType(), True),
    StructField("audience_rating", DoubleType(), True),
    StructField("audience_count", DoubleType(), True)
])


csv_path = "hdfs://hdfs-nn:9000/demo/bronze/Rotten Tomatoes Movies.csv"


df = spark.read.csv(
    csv_path,
    header=True,
    schema=titles_schema,
    multiLine=True,
    escape='"',
    quote='"',
    nullValue="",
    emptyValue=None
)

print(f"Total de registos lidos: {df.count()}")
df.show(5, truncate=50)



print("\n=== Início da Limpeza e Transformação ===")



df_clean = df.withColumn("in_theaters_date_str", trim(col("in_theaters_date")))
total_antes_filtro = df_clean.count()
df_clean = df_clean.filter(
    col("in_theaters_date_str").isNotNull() & (col("in_theaters_date_str") != "")
)
total_depois_filtro = df_clean.count()
print(f"TRATAMENTO (Filtro 'in_theaters_date'): Removidos {total_antes_filtro - total_depois_filtro} registos por data de estreia nula ou vazia.")




df_clean = df_clean.withColumn("in_theaters_date", to_date(col("in_theaters_date_str"), "yyyy-MM-dd"))


df_clean = df_clean.withColumn("on_streaming_date", to_date(trim(col("on_streaming_date")), "yyyy-MM-dd"))


df_clean = df_clean.withColumn("release_year", year(col("in_theaters_date")))


total_antes_filtro_ano = df_clean.count()
df_clean = df_clean.filter(col("release_year").isNotNull())
total_depois_filtro_ano = df_clean.count()
if (total_antes_filtro_ano - total_depois_filtro_ano) > 0:
    print(f"TRATAMENTO (Filtro 'release_year'): Removidos {total_antes_filtro_ano - total_depois_filtro_ano} registos por formato de data inválido.")


string_cols = [c.name for c in df_clean.schema.fields if c.dataType == StringType()]
for column in string_cols:
    df_clean = df_clean.withColumn(column, when(trim(col(column)) == "", None).otherwise(col(column)))
print("TRATAMENTO: Convertidas strings vazias (só espaços) para nulo.")

df_clean = df_clean.fillna({
    "movie_info": "Sem informação",
    "critics_consensus": "Sem informação",
    "rating": "Não classificado",
    "genre": "Desconhecido",
    "directors": "Desconhecido",
    "writers": "Desconhecido",
    "cast": "Desconhecido",
    "studio_name": "Desconhecido",
    "tomatometer_status": "Desconhecido",
    "runtime_in_minutes": 0,
    "tomatometer_rating": 0.0,
    "tomatometer_count": 0.0,
    "audience_rating": 0.0,
    "audience_count": 0.0
})
print("TRATAMENTO: Preenchidos valores nulos (strings e numéricos) com padrões.")




silver_path = f"{warehouse_location}/silver.db/rotten_tomatoes"
spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{warehouse_location}/silver.db'")

print(f"\n=== A guardar dados limpos em {silver_path} ===")

df_clean.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("release_year") \
    .save(silver_path)

spark.sql("DROP TABLE IF EXISTS silver.rotten_tomatoes")

spark.sql(f"""
    CREATE EXTERNAL TABLE silver.rotten_tomatoes (
        movie_title STRING,
        movie_info STRING,
        critics_consensus STRING,
        rating STRING,
        genre STRING,
        directors STRING,
        writers STRING,
        cast STRING,
        in_theaters_date DATE,
        on_streaming_date DATE,
        runtime_in_minutes INT,
        studio_name STRING,
        tomatometer_status STRING,
        tomatometer_rating DOUBLE,
        tomatometer_count DOUBLE,
        audience_rating DOUBLE,
        audience_count DOUBLE
    )
    PARTITIONED BY (release_year INT)
    STORED AS PARQUET
    LOCATION '{silver_path}'
""")

spark.catalog.recoverPartitions("silver.rotten_tomatoes")

print("\n Tabela Parquet particionada criada com sucesso em silver.rotten_tomatoes")

print("\n=== Contagem por ano ===")
spark.sql("""
    SELECT release_year, COUNT(*) AS total_titulos
    FROM silver.rotten_tomatoes
    GROUP BY release_year
    ORDER BY release_year DESC
    LIMIT 20
""").show()

print("\n=== Verificação de Datas de Streaming (Null vs Válidas) ===")
spark.sql("""
    SELECT 
        COUNT(*) AS total,
        COUNT(CASE WHEN on_streaming_date IS NULL THEN 1 END) AS com_streaming_nulo,
        COUNT(CASE WHEN on_streaming_date IS NOT NULL THEN 1 END) AS com_streaming_valido
    FROM silver.rotten_tomatoes
""").show()

spark.stop()

```

📌 Descrição do Script (ETL Rotten Tomatoes)

Este script PySpark executa um processo ETL (Extract, Transform, Load) sobre um dataset de filmes do portal Rotten Tomatoes. Os dados são lidos de um ficheiro CSV alojado no HDFS, limpos, transformados e posteriormente guardados no Data Lake na camada Silver, em formato Parquet e com particionamento por ano de lançamento.
Além disso, o script cria uma tabela externa Hive associada aos ficheiros Parquet previamente gerados.


🚀 Objetivo do ETL

Extrair dados brutos do ficheiro CSV armazenado no HDFS

Tratar e limpar dados inválidos, nulos ou com formato incorreto

Transformar campos (ex: datas) e criar atributos derivados (ex: ano de lançamento)

Guardar dados limpos na camada Silver em Parquet

Registar tabela em Hive para consulta SQL


🛠️ Tecnologias Utilizadas
| Tecnologia     | Utilização                                |
| -------------- | ----------------------------------------- |
| PySpark        | Processamento distribuído e transformação |
| Hive Metastore | Registo e gestão de tabelas externas      |
| HDFS           | Armazenamento distribuído                 |
| Parquet        | Formato otimizado de armazenamento        |
| Python         | Implementação da lógica ETL               |


🔎 Fluxo ETL

| Fase          | Descrição                                                      |
| ------------- | -------------------------------------------------------------- |
| **Extract**   | Leitura do CSV com schema definido                             |
| **Transform** | Limpeza, normalização, conversão de datas e criação de colunas |
| **Load**      | Escrita em Parquet particionado e criação de tabela Hive       |



🧩 Detalhes do Processo
1️⃣ Extração

Define manualmente o schema para garantir tipos corretos

Lê o CSV do HDFS com configuração robusta para evitar erros de parsing

Mostra uma amostra dos dados e conta o total de registos


2️⃣ Transformação

As transformações aplicadas incluem:

| Tipo         | Ação                                                     |
| ------------ | -------------------------------------------------------- |
| Validação    | Remove registos sem data de estreia (`in_theaters_date`) |
| Conversão    | Converte campos de data para `DateType`                  |
| Derivação    | Cria coluna `release_year` para particionamento          |
| Limpeza      | Substitui strings vazias ou espaços por valores `NULL`   |
| Normalização | Preenche valores nulos com defaults adequados            |


3️⃣ Carga

O script executa:

Armazenamento em Parquet particionado por release_year
Isto melhora desempenho na consulta, reduzindo leitura desnecessária.

Criação de tabela Hive externa
Mantém metadados no Hive Metastore apontando para ficheiros reais no HDFS.

Recuperação de partições com:

``` python

spark.catalog.recoverPartitions("silver.rotten_tomatoes")

```

🏁 Resultado Final

Após conclusão, ficas com:

✔ Dados limpos e tipados
✔ Dataset Parquet otimizado e particionado
✔ Tabela Hive pronta para Analytics ou BI
✔ Controlo de qualidade incluído




                       ┌───────────────────────────────┐
                       │       Fonte de Dados          │
                       │   Rotten Tomatoes Movies.csv  │
                       │    (Camada Bronze — HDFS)     │
                       └───────────────────────────────┘
                                       │
                                       ▼
                       ┌───────────────────────────────┐
                       │     Spark ETL Application     │
                       │   • Validação de dados         │
                       │   • Conversão de datas         │
                       │   • Normalização de nulos      │
                       │   • Derivação (release_year)   │
                       │   • Qualidade & Contagem       │
                       └───────────────────────────────┘
                                       │
                                       ▼
                       ┌───────────────────────────────┐
                       │ Camada Silver — Data Lake      │
                       │   Formato: Parquet             │
                       │   Particionado por: release_year│
                       │   Local: /warehouse/silver.db  │
                       └───────────────────────────────┘
                                       │
                                       ▼
                       ┌───────────────────────────────┐
                       │       Hive Metastore           │
                       │  Tabela Externa: silver.       │
                       │      rotten_tomatoes           │
                       │  Consultável por SQL engines   │
                       └───────────────────────────────┘
