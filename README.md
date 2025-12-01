# Pokémon VGC Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-orange?style=for-the-badge&logo=apachespark&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-success?style=for-the-badge)

## 📋 Descripción del Proyecto

Este proyecto es una **pipeline de ingeniería de datos** diseñada para analizar el "metagame" competitivo de Pokémon VGC (Video Game Championships). 

El objetivo principal no es solo la obtención de estadísticas, sino la implementación de una arquitectura escalable capaz de procesar grandes volúmenes de combinaciones de equipos, movimientos y estadísticas base utilizando **computación distribuida con PySpark**.

### 🎯 Objetivos Técnicos
* **Ingesta de Datos:** Extracción y limpieza de datos semi-estructurados de fuentes competitivas (Smogon).
* **Procesamiento Distribuido:** Uso de PySpark para transformar y agregar datos complejos (Usage stats, Teammates commonality).

---

## 🛠️ Tech Stack (Tecnologías)

* **Lenguaje:** Python 3.x
* **Procesamiento Big Data:** Apache Spark (PySpark)
* **Análisis Exploratorio:** Pandas, NumPy
* **Control de Versiones:** Git

---

## ⚙️ Arquitectura del Proyecto

El proyecto sigue una estructura ETL (Extract, Transform, Load) modular:

1.  **Raw Data Layer:** Ingesta de archivos JSON con datos de torneos y estadísticas de uso.
2.  **Processing Layer (Spark):** * Limpieza de valores nulos y normalización de nombres.
    * Cálculo de frecuencias de uso y sinergias entre equipos.
3.  **Presentation Layer:** Generación de reportes y visualizaciones listas para el análisis.

---

## 🚀 Instalación y Uso

Este proyecto está diseñado para ejecutarse dentro de VSCode en un entorno virtual.

### Prerrequisitos
* Python
* Pyspark
* Git

### Pasos para ejecutar

1.  **Clonar el repositorio:**
    Tambien se pueden descargar los archivos.

2.  **Extraccion de archivos**
    Extraer archivos json adentro de la carpeta 'chaos' en https://www.smogon.com/stats/ para los meses que se desean analisar

3.  **Crear el parquet:**
     Para crear el parquet se necesita ejecutar creacion_parquet_completo.py. Este archivo tiene que estar en el mismo lugar que todos los archivos .json que se         deseen analizar y al mismo nivel que creacion_individual_completa.py.

4.   **Analisis:**
     El ultimo paso seria usar los metodos de analisis.py para extraer la informacion que se busca.
