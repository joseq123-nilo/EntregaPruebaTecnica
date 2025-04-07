 
# Pipeline ETL con PySpark – Tienda de Alquiler de Películas 
Realizado por: Jose Manuel Quinchia Escobar. CC 1035236094

Este proyecto implementa un pipeline ETL usando **PySpark** para limpiar, transformar y analizar datos de una tienda de alquiler de películas. La información procesada incluye datos de películas, inventario, alquileres, clientes, y tiendas.

## Descripción General del Proceso

El pipeline realiza las siguientes etapas:

1. Carga de datos desde archivos CSV 
2. Revisión inicial de los DataFrames
3. Limpieza y transformación de datos
4. Renombramiento de columnas para evitar conflictos
5. Clasificación personalizada mediante la columna `return_status`
6. Integración de datos a través de joins entre tablas
7. Análisis exploratorio de datos (EDA), donde se realizán análisis de estadística descriptiva y análisis propios para formular preguntas de negocio
---

## 1. Carga de Datos

Se utilizan los siguientes archivos de entrada:

- `film.csv`
- `customer.csv`
- `inventory.csv`
- `rental.csv`
- `store.xlsx`

Los archivos CSV se cargan con `spark.read.csv()` y se convierte cada uno en DataFrame de Spark.

---

## 2. Revisión Inicial

Para cada DataFrame, se imprime:

- `.printSchema()` – estructura del esquema
- `.show(5)` – primeras 5 filas
- `.count()` – cantidad de registros
- `.columns` – nombres de columnas

---

## 3. Limpieza y Transformación

### Acciones generales:

- Se aplica una función que utiliza .trim() en todas las columnas de tipo StringType para eliminar espacios en blanco al inicio y al final de los valores de texto.

- Se implementa una función que reemplaza todos los valores 'NULL', 'Null' y cadenas vacías " " por None, unificando la representación de valores nulos en el DataFrame.

- Se ejecuta una función que recorre todas las columnas y elimina aquellas cuyo 100 % de los registros contienen valores nulos (None).

- Se aplica una función que evalúa todas las tablas y calcula el porcentaje de valores nulos por columna, con el objetivo de identificar problemas de completitud en los datos.

- Dado que las columnas customer_id_old y segment presentan aproximadamente un 43 % de valores nulos, y no aportan información crítica para el análisis, se decide eliminarlas del conjunto de datos.

- Limpieza de valores nulos en la tabla customer: Se detecta un único valor nulo en la columna last_name. Para evitar inconsistencias durante el análisis, se reemplaza dicho valor utilizando fillna({"last_name": "no last name"}).

- Estandarización de texto: Se implementa una función que convierte a mayúsculas todos los valores de columnas con tipo StringType, asegurando uniformidad en los datos de texto para evitar inconsistencias en análisis y comparaciones.

- Limpieza de columnas numéricas: Se desarroll funcion para limpiar columnas de valores enteros (IntegerType), eliminando caracteres no numéricos que puedan interferir con el análisis y la correcta asignación del tipo de dato a dichas columnas.

- Limpieza de columnas decimales: Se implementa una función para eliminar caracteres no numéricos en columnas con valores decimales, conservando únicamente la coma (,) como separador decimal. Adicionalmente, se analiza cada valor y, en caso de encontrar un punto (.) como separador, este se reemplaza por una coma para mantener consistencia en el formato numérico.

- Conversión de tipos de datos: Se realiza la conversión de tipos de datos en cada columna utilizando el método .cast(). Esta asignación se basa tanto en conocimientos propios como en la imagen del modelo entidad-relación (MER), que define los tipos de datos establecidos en el esquema original en SQL.

- Eliminación de duplicados: Se implementa una función que elimina las filas duplicadas en cada tabla utilizando el método .dropDuplicates(), asegurando que cada conjunto de datos contenga únicamente registros únicos para evitar distorsiones en el análisis posterior.

#### Ajustes específicos df `rental`

- Creación de columna `return_status`: Se implementa una función para generar la columna Return_Status, la cual se construye a partir de múltiples condicionales basadas en otras columnas. Esta lógica permite preservar filas con valores nulos que contienen información relevante para el análisis, evitando así la pérdida de datos importantes.-

## 4. Renombramiento de Columnas

Para evitar ambigüedad en los joins, se renombran varias columnas:

- last_update → last_update_film
- last_update → last_update_inventory
- last_update → last_update_rental
- last_update → last_update_customer
- last_update → last_update_store
- address_id → address_id_store
- address_id → address_id_customer
- store_id → store_id_store
- store_id → store_id_inventory

## 5. MER Modelado entidad-relación

- Se desarrolla una función que asegura el cumplimiento del Modelo Entidad-Relación proporcionado en el archivo excel. Esta función verifica la integridad referencial entre las tablas, garantizando que las relaciones definidas. 
- Construcción del DataFrame final `df_analitico_final`: Tras aplicar todas las transformaciones, limpiezas, validaciones de integridad y conversiones de tipo de dato, se crea el DataFrame consolidado llamado df_analitico_final. Este dataset integrado será la base principal sobre la cual se desarrollará todo el análisis exploratorio de datos (EDA).


## 7. Análisis Exploratorio de Datos (EDA)

### Para columnas numéricas (`IntegerType`, `ShortType`, `DecimalType`, `DoubleType`):

- Se calculan: cantidad de datos, media, desviación estándar, mínimo, máximo
- Cuartiles: Q1, mediana, Q3, Limite_Inferior, Limite_Superior y Outliers

### Para columnas categóricas (`StringType`):
- Cantidad de datos
- Datos unicos
- Top (Más frecuente)
- Frecuencia del valor

### Otros análisis de los datos

- Número total de películas, clientes, tiendas, alquileres
- ¿Cuántos clientes están activos vs. inactivos?
- Número de películas distintas por tienda
- Top 10 clientes con más alquileres
- Cantidad de alquileres por tienda
- Películas más alquiladas
- Promedio de alquileres por cliente
- Clientes que no devolvieron películas
- Duración promedio de alquileres (días)
- Evolución de alquileres en el tiempo (por mes)
- Valor de perdidas por peliculas no devueltas

