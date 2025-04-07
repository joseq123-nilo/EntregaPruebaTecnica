"
# Informe de Presentación de Resultados

## Introducción

Este informe presenta los resultados del desarrollo de un pipeline ETL (Extracción, Transformación y Carga) implementado en PySpark, aplicado a un conjunto de datos extraído del archivo Films2.xlsx.
El objetivo principal del proyecto fue procesar, limpiar, transformar y analizar los datos contenidos en varias tablas relacionadas con clientes, películas, inventarios, rentas y sucursales.

Como parte fundamental del proceso, se implementó un Modelo Entidad-Relación (MER) tomando como referencia la imagen incluida en el archivo fuente. A partir de los datos transformados 
se realizó un análisis exploratorio detallado y se generó conocimiento de valor para el negocio. Este documento resume las decisiones técnicas más relevantes, el análisis efectuado, 
y evidencia cómo los datos finales permiten responder preguntas clave de carácter estratégico para la organización.


## 1. Arquitectura de Datos y Arquetipo de la Aplicación

La arquitectura del proyecto sigue un enfoque modular de tipo ETL (Extracción, Transformación y Carga) empleando PySpark para el procesamiento distribuido de datos. 
A continuación se describen los componentes principales:

### Extracción:
- Del archivo `Films2.xlsx` se cargan cada una de las hojas como CSV `Film`,`Inventory`,`rental`,`customer`,`store`.
- Se usan funciones especializadas para convertir las hojas en DataFrames de PySpark.

### Transformación:
- Limpieza y estandarización de datos:
  - Eliminación de espacios con `.trim()` en columnas de tipo `StringType`.
  - Reemplazo de valores como `'NULL'`, `'Null'`, y `" "` por `None`.
  - Eliminación de columnas con 100% de valores nulos.
  - Cálculo del porcentaje de valores nulos por columna.
  - Eliminación de columnas no críticas (`customer_id_old` y `segment`) con más del 43% de datos nulos.
  - Conversión de todos los strings a mayúsculas.
  - Limpieza de caracteres no numéricos en columnas de enteros y decimales, respetando la coma como separador decimal.
  - Cambio de tipos de datos basado en conocimientos del dominio y el modelo entidad-relación original.
  - Eliminación de filas duplicadas.

- Enriquecimiento:
  - Creación de la columna `Return_Status`.
  - Construcción de relaciones entre tablas para asegurar la integridad del modelo MER mediante el uso de Join.
  - Creación del DataFrame `df_analitico_final` como base del análisis exploratorio.

### Carga:
- Los datos transformados están listos para ser cargados a un sistema analítico o de visualización.

## 2. Análisis Exploratorio de Datos

Se ha generado un resumen estadístico descriptivo que incluye la media, desviación estándar, mínimos, máximos, Q1 (primer cuartil), mediana (Q2) y Q3 (tercer cuartil) para cada una de las columnas relevantes del DataFrame df_analitico_final, incluyendo:
- `count`: número de valores no nulos.
- `unique`: número de valores únicos.
- `top`: valor más frecuente.
- `freq`: frecuencia del valor más frecuente.

# VARIABLES NUMERICAS (No se muestran todas las variables del Data Frame)
| summary | release_year | rental_duration | rental_rate |
|---------|--------------|-----------------|-------------|
| count   | 16044        | 16044           | 16044       |
| mean    | 2006.0       | 4.935489902767389 | 2.942630  |
| stddev  | 0.0          | 1.40168979439409  | 1.649677567954177 |
| min     | 2006         | 3                | 0.99       |
| max     | 2006         | 7                | 4.99       |

# VARIABLES CATEGORICAS (En la siguiente tabla no se muestran todas las varaibles)

| columna          | count | unique | top                | freq |
|------------------|-------|--------|---------------------|------|
| return_status     | 16044 | 3      | RETURNED            | 15861 |
| active            | 16044 | 2      | true                | 15640 |
| title             | 16044 | 958    | BUCKET BROTHERHOOD  | 34    |
| rating            | 16044 | 5      | PG-13               | 3585  |
| special_features  | 16044 | 4      | TRAILERS            | 8518  |

# DETECTAR OUTLIERS (Q1, Q2 , Q3 , Limite_Inferior, Limite_Superior)

| columna          | Q1     | Q2     | Q3     | Limite_Inferior | Limite_Superior | Outliers |
|------------------|--------|--------|--------|------------------|------------------|----------|
| length           | 80.0   | 112.0  | 148.0  | -22.0            | 250.0            | 0        |
| num_voted_users  | 18300.0| 38950.0| 58300.0| -41700.0         | 118300.0         | 0        |
| release_year     | 2006.0 | 2006.0 | 2006.0 | 2006.0           | 2006.0           | 0        |
| rental_duration  | 4.0    | 5.0    | 6.0    | 1.0              | 9.0              | 0        |
| rental_rate      | 0.99   | 2.99   | 4.99   | -5.01            | 10.99            | 0        |
| replacement_cost | 14.99  | 19.99  | 24.99  | -0.01            | 39.99            | 0        |


Adicionalmente, se analizan patrones como:

Número total de:
Películas: 958
Clientes: 599
Tiendas: 2
Alquileres: 16044

Cantidad de clientes activos vs. inactivos:

| active | count |
|--------|-------|
| true   | 15640 |
| false  | 404   |

Número de películas distintas disponibles por tienda:

| store_id_store | count |
|----------------|--------|
| 1              | 957    |
| 2              | 958    |

Top 10 clientes con más alquileres:

| customer_id | count |
|-------------|--------|
| 148         | 46     |
| 526         | 45     |
| 236         | 42     |
| 144         | 42     |
| 75          | 41     |
| 197         | 40     |
| 469         | 40     |
| 178         | 39     |
| 137         | 39     |
| 468         | 39     |

Cantidad de alquileres por tienda:

| store_id_store | count |
|----------------|--------|
| 1              | 8747   |
| 2              | 7297   |

Películas con mayor número de alquileres:

| title               | count |
|---------------------|--------|
| BUCKET BROTHERHOOD  | 34     |
| ROCKETEER MOTHER    | 33     |
| GRIT CLOCKWORK      | 32     |
| RIDGEMONT SUBMARINE | 32     |
| SCALAWAG DUCK       | 32     |
| JUGGLER HARDLY      | 32     |
| FORWARD TEMPLE      | 32     |
| TIMBERLAND SKY      | 31     |
| GOODFELLAS SALUTE   | 31     |
| NETWORK PEAK        | 31     |

Promedio de alquileres por cliente:

| avg_rentals        |
|--------------------|
| 26.784641068447414 |

Total de clientes con películas no devueltas: 1

| customer_id |
|-------------|
| 554         |

Duración promedio de los alquileres en días:

| promedio_dias      |
|--------------------|
| 5.025219090851775  |

Ingreso potencial perdido por películas con estado LOST:
Perdidas por peliculas no devueltas: $0.99

Los resultados del análisis revelan tendencias clave que pueden orientar decisiones estratégicas y operativas dentro de la organización.

## 3. Preguntas de Negocio y Respuestas

**1. ¿Cuál es la película más alquilada y cuántas veces ha sido rentada?**  
Respuesta: Se identificó  BUCKET BROTHERHOOD - 34 alquileres, mediante el análisis de la columna `Tittle` y su frecuencia. La película con más rentas aparece con una frecuencia significativamente mayor al promedio, 
lo que podría justificar un aumento de copias en inventario.

**2. ¿Qué cliente ha generado más ingresos a la empresa?**  
Respuesta: El cliente identificado con customer_id 148 ha realizado 46 alquileres, Al agrupar por `customer_id` y contar la cantidad de apariciones, se determinó el cliente más rentable. 
Esta información es clave para estrategias de fidelización.

**3. ¿Qué sucursal tiene mayor volumen de transacciones?**  
Respuesta: La columna `store_id` permitió identificar que la sucursal 1 cuenta con  8747 alquileres en total en comparación con la sucursal 2 que cuenta con 7297, 
lo que puede influir en la distribución de recursos y personal.

**4. ¿Cuántas películas rentadas no han sido devueltas?**  
Respuesta: Utilizando la columna `Return_Status`, se contabilizaron los casos donde el valor representa `No Return`, revelando una tasa de no devolución que podría impactar 
las políticas de garantía o penalización, apenas uno de los clientes no ha cumplido con la devolución

**5. ¿Exisiten outliers entre los datos númericos?
Respuesta: Tras aplicar métodos de detección de valores atípicos (outliers) en las columnas numéricas, se identificó que no existen datos que puedan considerarse fuera del rango esperado. 
Esto puede interpretarse como una señal de que las variables analizadas tienen poca variabilidad o presentan una distribución uniforme. Este resultado también puede deberse a una limpieza 
previa de los datos, a una baja dispersión natural en las variables numéricas, o a que el método de detección empleado (como el IQR) no encontró diferencias significativas.

## 4. Conclusiones

- La arquitectura ETL implementada con PySpark permite una gestión eficiente y escalable de grandes volúmenes de datos.
- La limpieza rigurosa de datos garantiza su confiabilidad para futuros análisis e implementación de modelos de ML.
- Se logró construir un DataFrame analítico robusto (`df_analitico_final`) sobre el cual se puede aplicar cualquier técnica de análisis descriptivo o predictivo.
- Los datos permiten responder a preguntas clave del negocio, lo que respalda la toma de decisiones basada en evidencia.
- El uso del modelo entidad-relación original como guía asegura coherencia estructural en la integración de las tablas.

