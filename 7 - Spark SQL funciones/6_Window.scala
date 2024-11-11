// Funciones de ventana

val df = spark.read.parquet("/FileStore/section9/funciones_ventana.parquet")

df.show(false)

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions.{desc, row_number, rank, dense_rank}

// Definir la ventana por la que se va a particionar y ordenar
val windowSpec = Window.partitionBy("departamento").orderBy(desc("puntos"))

// row_number se especifica sobre una ventana. Enumera cada fila dentro de la partición
df.withColumn("row_number", row_number().over(windowSpec)).show(false)

// rank se especifica sobre una ventana. Crea un ranking de filas. En caso de empate se salta posiciones
df1.withColumn("rank", rank().over(windowSpec)).show

// dense_rank se especifica sobre una ventana. Crea un ranking de filas. En caso de empate no se salta posiciones
df1.withColumn("dense_rank", dense_rank().over(windowSpec)).show

// Agregación con funciones de ventana

val windowSpecAgg = Window.partitionBy("departamento")

import org.apache.spark.sql.functions.{col, min, max, avg}

df.withColumn("min", min(col("puntos")).over(windowSpecAgg))
  .withColumn("max", max(col("puntos")).over(windowSpecAgg))
  .withColumn("avg", avg(col("puntos")).over(windowSpecAgg))
  .withColumn("row_number", row_number().over(windowSpec))
.show