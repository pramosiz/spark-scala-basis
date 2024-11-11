// Funciones para trabajo con colecciones

/* /FileStore/section9/formato_array.parquet
   /FileStore/section9/formato_json.parquet */

val dfArray = spark.read.parquet("/FileStore/section9/formato_array.parquet")

dfArray.printSchema

dfArray.show(false)

// Obtener el tamaño del arreglo, ordenarlo y verificar si existe un valor en el arreglo

import org.apache.spark.sql.functions.{col, size, sort_array, array_contains, explode}

dfArray.select(
  size(col("tareas")).as("longitud"), // size para obtener el tamaño del array
  sort_array(col("tareas")).as("arr_ordenado"), // sort_array para ordenar el array de menor a mayor, en este caso alfabéticamente
  array_contains(col("tareas"), "buscar agua").as("buscar_agua") // array_contains para verificar si existe un valor en el array
).show(false)

// La función explode creará una nueva fila para cada elemento del array
// Originamente teníamos 1 filas, ahora tendremos 3 porque
// la columna tareas era un array con 3 elementos
dfArray.select(
  col("dia"),
  explode(col("tareas")).as("tareas")
).show(false)

// Trabajo con JSON

// Su contenido es un único string JSON
val dfJson = spark.read.parquet("/FileStore/section9/formato_json.parquet")

dfJson.printSchema

dfJson.show(false)

import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType}

// Definir el esquema del JSON
val schemaJson = StructType(Array(
  StructField("dia", StringType, true),
  StructField("tareas", ArrayType(StringType), true)
))

// Usar from_json para convertir el string JSON

import org.apache.spark.sql.functions.{from_json, to_json}

// Creamos el DataFrame con el esquema definido
val dfJSON = dfJson.select(
  from_json(col("tareas_str"), schemaJson).as("por_hacer")
)

dfJSON.printSchema

// getItem para obtener elementos

dfJSON.select(
  col("por_hacer").getItem("dia"),
  col("por_hacer").getItem("tareas").getItem(0).as("primera_tarea"),
  col("por_hacer").getItem("tareas").getItem(1).as("segunda_tarea")
).show(false)

// to_json para convertir un DataFrame a String json

dfJSON.select(
  to_json(col("por_hacer"))
).show(false)

