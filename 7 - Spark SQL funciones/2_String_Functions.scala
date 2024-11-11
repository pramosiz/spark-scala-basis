// Funciones para trabajo con strings

val df = spark.read.parquet("/FileStore/section9/strings.parquet")

df.printSchema

df.show(false)

// trim, ltrim, rtrim

import org.apache.spark.sql.functions.{col, trim, ltrim, rtrim}

df.select(
  col("nombre"),
  ltrim(col("nombre")).as("lt"), // elimina espacios a la izquierda
  rtrim(col("nombre")).as("rt"), // elimina espacios a la derecha
  trim(col("nombre")).as("tr")   // elimina espacios a ambos lados
).show(false)

// lpad y rpad

import org.apache.spark.sql.functions.{lpad, rpad}

df.select(
  col("nombre"),
  lpad(trim(col("nombre")), 8, "-").as("lp"), // rellena con "-" a la izquierda hasta completar 8 caracteres
  rpad(trim(col("nombre")), 8, "=").as("rp")  // rellena con "=" a la derecha hasta completar 8 caracteres
).show(false)

// concatenación, mayúscula, minúscula y reversa

import org.apache.spark.sql.functions.{concat_ws, lower, upper, initcap, reverse}

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase") // Concatenación de Strings
).select(
  col("frase"),
  lower(col("frase")).as("lw"),   // lower case
  upper(col("frase")).as("up"),   // upper case
  initcap(col("frase")).as("ic"), // Inicial en mayúscula de cada palabra
  reverse(col("frase")).as("rev") // Reversa
).show(false)

// regexp_replace

import org.apache.spark.sql.functions.regexp_replace

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  regexp_replace(col("frase"), "Spark|es", "lindo").as("regexp") // Busca las palabras Spark o es y los reemplaza por lindo
).show(false)