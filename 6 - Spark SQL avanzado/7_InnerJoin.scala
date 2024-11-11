// Inner Joins

/* /FileStore/section8/departamentos.parquet
   /FileStore/section8/empleados.parquet */

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show

departamentos.show

// Inner join

import org.apache.spark.sql.functions.col

// Junta los dos dataframes por la columna num_dpto e id. Los datos que no
// coincidan en ambas columnas no se añadirán al nuevo dataframe.
// El DataFrame resultante tendrá las columnas de ambos DataFrames, incluyendo
// num_dpto e id, que son las columnas que se han utilizado para hacer el join.
val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"))

joinDF.show

// La parte "inner" es opcional, ya que es el valor por defecto. Se refiere
// al tipo de join que se va a realizar. En este caso, se ha realizado un inner join.
val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"), "inner")

joinDF.show