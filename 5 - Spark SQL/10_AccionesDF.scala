// Acciones sobre un dataframe en Spark SQL

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.show()

df.show(5, false) // Muestra 5 filas y no trunca las columnas

// Muestras el contenido de 5 filas de la columna "titles" no truncando las columnas 
df.select("title").show(5, false) 

df.head(1) // Devuelve un array con la primera fila

df.take(1) // Devuelve un array con la primera fila. Head puede llamarse sin argumentos

df.select("likes").collect

df.count