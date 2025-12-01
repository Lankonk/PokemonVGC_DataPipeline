from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, sum as spark_sum, split
import matplotlib.pyplot as plt
import pandas as pd

# Iniciar Spark
def iniciar_spark():
    spark = SparkSession.builder \
        .appName("VGC_Analisis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    ruta_parquet = "vgc_data_parquet_completo"
    print("Cargando datos...")
    df = spark.read.parquet(ruta_parquet) #toda la carpeta es una sola tabla
    return df, spark

def consultar_schema(df):
    print("--- ESQUEMA DE DATOS ---")
    df.printSchema()

def registros_por_fecha(df):
    print("--- REGISTROS POR FECHA ---")
    df.groupBy("Date").count().orderBy("Date").show(20)

def pokemon_unicos(df):
    res = df.select("Pokemon").distinct().count()
    print(f"Número de pokemones únicos: {res}")

#Funciones para analisis
#1.  
#Nota: Pyspark no toma en cuenta valores nulos en los calculos de promedio, por lo que existe cierto sesgo en los resultados
def esperanza_pokemones_mas_usado(df):
    print("--- ESPERANZA DE USO DE POKEMONES ---")
    return df.groupBy("Pokemon").mean("Usage_Percent").withColumnRenamed("avg(Usage_Percent)", "Promedio_Uso") \
    .orderBy(desc("Promedio_Uso"))

#elimina el sesgo al filtrar por fechas
def esperanza_pokemones_mas_usados_rango_fecha(df, fecha_inicio, fecha_fin):
    print(f"--- ESPERANZA DE USO DE POKEMONES ENTRE {fecha_inicio} Y {fecha_fin} ---")
    df_fechas =df.filter((col("Date") >= fecha_inicio) & (col("Date") <= fecha_fin))
    return esperanza_pokemones_mas_usado(df_fechas)

#menos sesgo debido a que no se usa el porcentaje
def n_pokemones_mas_usados_numero(df):
    print("--- POKEMONES MAS USADOS POR NUM ---")
    return df.groupBy("Pokemon").agg(spark_sum("Raw_Count").alias("Total_Usos")) \
    .orderBy(desc("Total_Usos"))

def n_pokemones_mas_usados_rango_fecha_num(df, fecha_inicio, fecha_fin):
    print(f"--- POKEMONES MAS USADOS POR NUM ENTRE {fecha_inicio} Y {fecha_fin} ---")
    df_fechas =df.filter((col("Date") >= fecha_inicio) & (col("Date") <= fecha_fin))
    return n_pokemones_mas_usados_numero(df_fechas)

#2
def aliados_mas_comunes_a_pkmn(df, nom_pkmn):
    df_pkmn = obtener_df_pokemon(df, nom_pkmn)
    print(f"--- ALIADOS MAS COMUNES DE {nom_pkmn} ---")
    df_aliados = aliados_df(df_pkmn)
    res = df_aliados.groupBy("Teammate_Name").agg(spark_sum("Teammate_Count").alias("Total_Usos")) \
    .orderBy(desc("Total_Usos"))
    return res

#3
#sin fechas
def items_mas_usados(df,nom_pkmn):
    df_pkmn = obtener_df_pokemon(df, nom_pkmn)
    df_items = items_df(df_pkmn)
    print(f"--- ITEMS MAS USADOS POR {nom_pkmn} ---")
    res = df_items.groupBy("Item_Name").agg(spark_sum("Item_Count_Raw").alias("Total_Usos")) \
    .orderBy(desc("Total_Usos"))
    return res

#4
#numero ponderado de "que tan bueno es un ataque"
def historial_ataque(df,nom_ataque):
    df_moves = movimientos_df(df)
    ataque_buscado = nom_ataque.lower().replace(" ", "").replace("-", "")
    print(f"--- HISTORIAL DE USO DEL ATAQUE {nom_ataque} ---")
    return df_moves.filter(col("Move_Name") == ataque_buscado).groupBy("Date") \
    .agg(spark_sum("Move_Count").alias("Total_Usos")).orderBy("Date")
#5
def ataques_mas_usados(df,nom_pkmn):
    df_pkmn = obtener_df_pokemon(df, nom_pkmn)
    df_moves = movimientos_df(df_pkmn)
    print(f"--- ATAQUES MAS USADOS POR {nom_pkmn} ---")
    res = df_moves.groupBy("Move_Name").agg(spark_sum("Move_Count").alias("Total_Usos")) \
        .orderBy(desc("Total_Usos"))
    return res

def natures_mas_usados_pkmn(df,nom_pkmn):
    df_pkmn = obtener_df_pokemon(df, nom_pkmn) 
    print(f"--- NATURES MAS USADOS POR {nom_pkmn} ---")
    temp = df_pkmn.select(explode(col("Spreads_Map")).alias("Spread_Full_String", "Spread_Count"))
    df_nature = temp.withColumn("Nature_Name", split(col("Spread_Full_String"), ":").getItem(0))
    # 4. Agrupamos por la nueva columna extraída
    res = df_nature.groupBy("Nature_Name").agg(spark_sum("Spread_Count").alias("Total_Usos")) \
                         .orderBy(desc("Total_Usos"))
    return res

def spreads_mas_usados_pkmn(df,nom_pkmn):
    df_pkmn = obtener_df_pokemon(df, nom_pkmn)
    print(f"--- SPREADS MAS USADOS POR {nom_pkmn} ---")
    res = df_pkmn.select(explode(col("Spreads_Map")).alias("Spread_Name", "Spread_Count"))
    return res.groupBy("Spread_Name").agg(spark_sum("Spread_Count").alias("Total_Usos")) \
        .orderBy(desc("Total_Usos"))

#graficas

#historial de popularidad de un ataque (todos los pokemones)
def graficar_historial_ataque(df,nom_ataque,n=134):
    df_historial = historial_ataque(df, nom_ataque).toPandas()
    df_historial["Date"] = pd.to_datetime(df_historial["Date"])
    df_historial = df_historial.sort_values("Date")
    plt.figure(figsize=(12,6))
    plt.plot(df_historial["Date"], df_historial["Total_Usos"], marker='o')
    plt.title(f'Historial de uso del ataque: {nom_ataque}')
    plt.xlabel('Fecha')
    plt.ylabel('Usos Ponderados')
    plt.xticks(rotation=45)
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.grid(True,alpha=0.3)
    plt.show()

#grafica el uso de un pokemon en el tiempo
def graficar_uso_pokemones(df,nombres_pkmn,fecha_inicio="2014-11", fecha_fin="2025-10"): 
    df_fechas = df.filter((col("Date") >= fecha_inicio) & (col("Date") <= fecha_fin))
    plt.figure(figsize=(14,7))
    
    for nom_pkmn in nombres_pkmn:
        df_pkmn = obtener_df_pokemon(df_fechas, nom_pkmn)
        df_uso = df_pkmn.groupBy("Date").mean("Usage_Percent").withColumnRenamed("avg(Usage_Percent)", "Promedio_Uso").orderBy("Date")
        df_uso_pd = df_uso.toPandas()
        df_uso_pd["Promedio_Uso"] = (df_uso_pd["Promedio_Uso"] / 2) * 100  #ajuste para escala de porcentaje
        df_uso_pd["Date"] = pd.to_datetime(df_uso_pd["Date"])
        df_uso_pd = df_uso_pd.sort_values("Date")
        plt.plot(df_uso_pd["Date"], df_uso_pd["Promedio_Uso"], marker='o', label=nom_pkmn)
    
    plt.title(f'Uso de Pokémon en el tiempo')
    plt.xlabel('Fecha')
    plt.ylabel('Porcentaje de Uso Promedio')
    plt.xticks(rotation=45)
    plt.gcf().autofmt_xdate()
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

def histograma_aliados_pokemon(df, nom_pkmn, n=10):
    df_aliados = aliados_mas_comunes_a_pkmn(df, nom_pkmn).limit(n)
    df_aliados_pd = df_aliados.toPandas()
    
    plt.figure(figsize=(12,6))
    plt.barh(range(len(df_aliados_pd)), df_aliados_pd["Total_Usos"], color='purple')
    plt.yticks(range(len(df_aliados_pd)), df_aliados_pd["Teammate_Name"])
    plt.xlabel('Total de Usos como Aliado')
    plt.ylabel('Pokémon Aliado')
    plt.title(f'Top {n} aliados más comunes de {nom_pkmn}')
    plt.gca().invert_yaxis()
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.show()


# grafica de los n pokemones mas usados por porcentaje
def graficar_top_pokemon_porcentaje(df, n=20):
    df_top = esperanza_pokemones_mas_usado(df).limit(n)
    df_top_pd = df_top.toPandas()
    df_top_pd["Promedio_Uso"] = (df_top_pd["Promedio_Uso"] / 2) * 100  #ajuste para escala de porcentaje
    
    plt.figure(figsize=(12,8))
    plt.barh(range(len(df_top_pd)), df_top_pd["Promedio_Uso"], color='steelblue')
    plt.yticks(range(len(df_top_pd)), df_top_pd["Pokemon"])
    plt.xlabel('Porcentaje de Uso Promedio')
    plt.ylabel('Pokémon')
    plt.title(f'Top {n} Pokémon más usados (por porcentaje promedio)')
    plt.gca().invert_yaxis()  # Mostrar el más usado arriba
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.show()

# grafica de los n pokemones mas usados por porcentaje en rango de fechas
def graficar_top_pokemon_porcentaje_rango(df, fecha_inicio, fecha_fin, n=20):
    df_top = esperanza_pokemones_mas_usados_rango_fecha(df, fecha_inicio, fecha_fin).limit(n)
    df_top_pd = df_top.toPandas()
    df_top_pd["Promedio_Uso"] = (df_top_pd["Promedio_Uso"] / 2) * 100
    
    plt.figure(figsize=(12,8))
    plt.barh(range(len(df_top_pd)), df_top_pd["Promedio_Uso"], color='coral')
    plt.yticks(range(len(df_top_pd)), df_top_pd["Pokemon"])
    plt.xlabel('Porcentaje de Uso Promedio')
    plt.ylabel('Pokémon')
    plt.title(f'Top {n} Pokémon más usados ({fecha_inicio} a {fecha_fin})')
    plt.gca().invert_yaxis()
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.show()

# grafica de los n items más usados por un pokemon
def graficar_items_pokemon(df, nom_pkmn, n=10):
    df_items = items_mas_usados(df, nom_pkmn).limit(n)
    df_items_pd = df_items.toPandas()
    
    plt.figure(figsize=(12,6))
    plt.barh(range(len(df_items_pd)), df_items_pd["Total_Usos"], color='gold')
    plt.yticks(range(len(df_items_pd)), df_items_pd["Item_Name"])
    plt.xlabel('Total de Usos')
    plt.ylabel('Item')
    plt.title(f'Top {n} items más usados por {nom_pkmn}')
    plt.gca().invert_yaxis()
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.show()

# grafica de los n ataques mas usados por un pokenon
def graficar_ataques_pokemon(df, nom_pkmn, n=10):
    df_ataques = ataques_mas_usados(df, nom_pkmn).limit(n)
    df_ataques_pd = df_ataques.toPandas()
    
    plt.figure(figsize=(12,6))
    plt.barh(range(len(df_ataques_pd)), df_ataques_pd["Total_Usos"], color='crimson')
    plt.yticks(range(len(df_ataques_pd)), df_ataques_pd["Move_Name"])
    plt.xlabel('Total de Usos')
    plt.ylabel('Ataque')
    plt.title(f'Top {n} ataques más usados por {nom_pkmn}')
    plt.gca().invert_yaxis()
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.show()


#funciones para obtener dataframes filtrados

def obtener_df_pokemon(df,nombre_pkmn):
    return df.filter(col("Pokemon") == nombre_pkmn)

#con fechas
def items_df(df):
    return df.select(col("Date"),explode(col("Items_Map")).alias("Item_Name", "Item_Count_Raw"))

def movimientos_df(df):
    return df.select(col("Date"),explode(col("Moves_Map")).alias("Move_Name", "Move_Count"))

def aliados_df(df):
    return df.select(col("Date"),explode(col("Teammates_Map")).alias("Teammate_Name", "Teammate_Count"))

