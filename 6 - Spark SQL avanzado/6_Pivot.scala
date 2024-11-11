// Agregación con pivote

val df = spark.read.parquet("/FileStore/section8/estudiantes.parquet")

df.show

// Pivot se utiliza en análisis o presentación de datos.
// Con Pivot podemos agrupar los datos de las filas en columnas nuevas
// Por ejemplo, si tenemos datos con el sexo de los estudiantes, podemos
// agrupar los datos por sexo y mostrar el peso promedio de los estudiantes
df.groupBy("graduacion") // Agrupamos por graduación
    .pivot("sexo")       // Transformamos nuestras filas en columnas
    .agg(avg(col("peso"))).show

df.groupBy("graduacion").pivot("sexo").agg(
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show

// Filtra solo los estudiantes masculinos. Es una buena práctica especificar
// los valores por los que queremos hacer las agregaciones
df.groupBy("graduacion").pivot("sexo", Array("M")).agg(
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show
