from pyspark.sql import SparkSession

print("--- Iniciando Transformação SparkSQL (Camada Silver) ---")
spark = SparkSession.builder.appName("ETL-Silver").getOrCreate()

# 1. Lê o arquivo da Bronze
df = spark.read.csv("/home/jovyan/data/bronze/vehicles.csv", header=True, inferSchema=True)

# 2. Cria a View Temporária para usar SQL puro (Regra do Professor)
df.createOrReplaceTempView("carros_brutos")

# 3. Limpeza Inicial: Seleciona colunas e remove preços absurdos (Pré-processamento)
df_silver = spark.sql("""
    SELECT 
        id, 
        CAST(price AS DOUBLE) as price, 
        year, 
        manufacturer, 
        model, 
        condition,
        CAST(odometer AS DOUBLE) as odometer,
        transmission,
        state
    FROM carros_brutos 
    WHERE price > 500 AND price < 150000 
      AND year IS NOT NULL 
      AND odometer IS NOT NULL
""")

# 4. Salva em Parquet na Silver (Cria a pasta automaticamente)
destino_silver = "/home/jovyan/data/silver/vehicles_parquet"
df_silver.write.mode("overwrite").parquet(destino_silver)

print(f"Sucesso: Dados processados e salvos em Parquet em {destino_silver}")
spark.stop()