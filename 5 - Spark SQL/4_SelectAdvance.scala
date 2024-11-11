// Funciones select y selectExpr

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.printSchema // Para ver el esquema del dataframe

import org.apache.spark.sql.functions.col

df.select(col("video_id")).show()

df.select("video_id", "title").show()

// Esta via da error

df.select(
  "likes",
  "dislikes",
  "likes" - "dislikes" // Esto da error porque no existe en ese DataFrame
).show()

//La forma correcta

df.select(
  col("likes"),
  col("dislikes"),
  (col("likes") - col("dislikes")).as("aceptacion") // Esto si funciona
).show()

// selectExpr

df.selectExpr("likes", "dislikes", "(likes - dislikes) as aceptacion").show() // Esto es lo mismo que el anterior

df.selectExpr("count(distinct(video_id)) as videos").show()