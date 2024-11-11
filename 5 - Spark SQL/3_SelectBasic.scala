// Trabajo con columnas. Operaciones más relacionales

val df = spark.read.parquet("/FileStore/section7/dataPARQUET.parquet")

// Primera alternativa para referirse a las columnas

df.printSchema

df.select("title").show(false) // Selecciona la columna title. El parámetro false es para que no trunque el contenido

// Segunda alternativa

import org.apache.spark.sql.functions.{col, column}

df.select(col("title")).show()

df.select(column("title")).show()

// Tercera alternativa

df.select($"title").show()