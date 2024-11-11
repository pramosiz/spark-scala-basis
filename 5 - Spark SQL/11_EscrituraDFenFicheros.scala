// Escritura de DataFrames

val df = spark.read.parquet("/FileStore/section7/lectura44/datos.parquet")

// Escribir el df como csv cambiándole el delimitador
df.write                 // DataFrameWriter
    .format("csv")       // Formato de salida
    .option("sep", "|")  // key-value options. sep = separador, se separa con | (pipe)
    .save("/FileStore/section7/lectura44/csv") // Directorio de salida

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// El número de archivos escritos en el directorio de salida corresponde al número de particiones que tiene un DataFrame

df.rdd.getNumPartitions // Solo tendrá 1 partición porque solo tiene 1 archivo

df.repartition(2) // Se reparticiona el DataFrame en 2 particiones
    .write
    .format("csv")
    .option("sep", "|")
    .mode("overwrite") // Sobreescribe el directorio de salida
    .save("/FileStore/section7/lectura44/csv")

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// Particionando los datos

df.printSchema

import org.apache.spark.sql.functions.col

df.select(col("comments_disabled")).distinct.show

// Filtra los datos que tengan en la columna comments_disabled los valores "False" o "True"
val dfFiltrado = df.filter(col("comments_disabled").isin("False", "True"))

dfFiltrado.write
    .partitionBy("comments_disabled") // Particiona los datos por la columna comments_disabled
    // Por lo tanto crea tantas carpetas como valores distintos haya especificados en partitionBy en la parte de coments_disabled
    .parquet("/FileStore/section7/lectura44/parquet") // Escribe los datos en formato parquet
// Al hacer esto, se crean 2 carpetas en el directorio de salida, 
// una para cada valor de la columna comments_disabled (False y True)

dbutils.fs.ls("/FileStore/section7/lectura44/parquet")
