// Manejo de nombres de columna duplicados

val empleados= spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

// Renombramos la columna num_dpto a id en el DataFrame empleados
// para que coincida el nombre de la columna con el DataFrame departamentos
val empleadosRen = empleados.withColumnRenamed("num_dpto", "id")

empleadosRen.show

// Si en este punto trataramos de hacer un join como el siguiente nos daría error

import org.apache.spark.sql.functions.col

empleadosRen.join(departamentos, col("id") === col("id")).show // Esto da error porque piensa que es ambiguo

// Esto sí que funciona
val dfDuplicados = empleadosRen.join(departamentos, empleadosRen.col("id") === departamentos.col("id"))

dfDuplicados.show

import org.apache.spark.sql.functions.col

dfDuplicados.select(col("id")).show // Esto da error, el mismo que antes por ser ambiguo

// Esto sí que funciona
dfDuplicados.select(empleadosRen.col("id")).show

// Para evitar duplicados, especificamos en un Seq las columnas por 
// las que queremos hacer el join. Así solo se mostrará una vez la columna id
val sinDuplicados = empleadosRen.join(departamentos, Seq("id"))

sinDuplicados.show