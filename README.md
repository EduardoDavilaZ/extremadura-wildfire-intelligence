# Extremadura Wildfire Intelligence: Geospatial Lakehouse

Pipeline de procesamiento de Big Data y análisis geoespacial orientado a la gestión histórica y a la evaluación del riesgo de propagación de incendios forestales en la Comunidad Autónoma de Extremadura (España).

## 🚀 Descripción del Proyecto
Este proyecto implementa una arquitectura moderna de **Data Lakehouse** utilizando **Databricks**, **Apache Spark (PySpark)** y **Delta Lake**. Su objetivo principal es unificar fuentes de datos heterogéneas (registros históricos oficiales del MITECO y detecciones satelitales) para transformar datos sin procesar en información analítica de valor para la prevención de incendios.

## 🏛️ Arquitectura (Medallion Architecture)
El flujo de datos se divide en tres capas bien diferenciadas:
1. **Bronze (Raw):** Almacenamiento inmutable de los datos originales tal y como se obtienen de las fuentes (XML del MITECO, CSV de FIRMS/MODIS).
2. **Silver (Clean & Conformed):** Datos limpios, normalizados, con esquemas validados y filtrados geográficamente para Extremadura.
3. **Gold (Aggregated & Analytical):** Tablas optimizadas y estructuradas (en formato Delta Lake) listas para alimentar modelos de Machine Learning y paneles de visualización.

## 🛠️ Stack Tecnológico
* **Plataforma Big Data:** Databricks / Apache Spark
* **Procesamiento Distribuido:** PySpark & Python
* **Formato de Almacenamiento:** Delta Lake & Parquet
* **Análisis Geoespacial:** GeoPandas, Shapely
* **Control de Versiones:** Git & GitHub

## 📂 Estructura del Repositorio
```text
extremadura-wildfire-intelligence/
│
├── .github/
│   └── workflows/                # CI/CD y automatización de pruebas
│
├── config/                       # Archivos de configuración y esquemas
│
├── notebooks/                    # Notebooks de Databricks para exploración y ETL
│   ├── 01_egif_exploration.ipynb
│   ├── 02_firms_exploration.ipynb
│   └── 03_data_quality.ipynb
│
├── src/                          # Código fuente modular reutilizable
│   ├── ingestion/                # Scripts de ingesta de datos
│   ├── transformations/          # Lógica de limpieza y cambios Bronze -> Silver -> Gold
│   ├── validation/               # Controles de calidad de datos
│   └── utils/                    # Funciones auxiliares de Spark y conexiones
│
├── tests/                        # Pruebas unitarias con PyTest
├── docs/                         # Documentación y diagramas
├── requirements.txt              # Dependencias de Python
└── README.md