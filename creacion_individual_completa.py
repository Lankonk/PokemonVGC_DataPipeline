import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, coalesce
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, FloatType

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No se especificó archivo")
        sys.exit(1)
        
    ruta = sys.argv[1]

    # --- 1.  ESQUEMA COMPLETO  ---
    
    stats_schema = StructType([
        StructField("Raw count", IntegerType(), True),
        StructField("Usage", FloatType(), True),
        # Mapas de detalles
        StructField("Abilities", MapType(StringType(), FloatType()), True), # Ojo: Abilities suelen ser float o int
        StructField("Items", MapType(StringType(), FloatType()), True),
        StructField("Moves", MapType(StringType(), FloatType()), True),
        StructField("Teammates", MapType(StringType(), FloatType()), True),
        StructField("Nature", MapType(StringType(), FloatType()), True),
        StructField("Spreads", MapType(StringType(), FloatType()), True) # <--- AGREGADO
    ])

    json_schema = StructType([
        StructField("info", StructType([
            StructField("number of battles", IntegerType(), True)
        ])),
        StructField("data", MapType(StringType(), stats_schema)) 
    ])
    # ---------------------------------------------------------

    spark = SparkSession.builder \
        .appName(f"Worker_{os.path.basename(ruta)}") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    try:
        nombre_archivo = os.path.basename(ruta) 
        fecha_extraida = nombre_archivo.split('_')[0] 
        
        # 2. LEER CON ESQUEMA
        raw_df = spark.read \
            .option("multiLine", "true") \
            .schema(json_schema) \
            .json(ruta)
        
        if raw_df.rdd.isEmpty():
            print("ARCHIVO VACIO")
            spark.stop()
            sys.exit(0)

        # 3. LÓGICA DE NEGOCIO
        try:
            row = raw_df.select(col("info.`number of battles`")).head()
            if row is None: raise Exception("Info vacia")
            total_battles = row[0]
        except:
            total_battles = 1 

        exploded_df = raw_df.select(
            explode(col("data")).alias("Pokemon", "Stats")
        )

        # 4. SELECCIÓN FINAL
        final_df = exploded_df.select(
            col("Pokemon"),
            col("Stats.`Raw count`").alias("Raw_Count"),
            col("Stats.Usage").alias("Usage_Original"),
            # Mapas detallados
            col("Stats.Abilities").alias("Abilities_Map"),
            col("Stats.Items").alias("Items_Map"),
            col("Stats.Moves").alias("Moves_Map"),
            col("Stats.Teammates").alias("Teammates_Map"),
            col("Stats.Nature").alias("Nature_Map"),
            col("Stats.Spreads").alias("Spreads_Map")
        ).withColumn(
            "Usage_Percent", 
            col("Raw_Count") / lit(total_battles)
        ).withColumn(
            "Date", lit(fecha_extraida)
        ).withColumn( 
            "Source_File", lit(ruta) 
        )

        # 5. GUARDAR
        final_df.write.mode("append").parquet("vgc_data_parquet_completo")
        
        print(f"EXITO: {ruta}")

    except Exception as e:
        sys.stderr.write(str(e))
        print(f"ERROR: {e}")
        
    finally:
        spark.stop()