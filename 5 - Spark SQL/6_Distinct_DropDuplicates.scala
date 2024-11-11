// Funciones distinct y dropDuplicates

val df = spark.read.parquet("/FileStore/section7/data.parquet")

df.show

df.distinct.show // Elimina filas ENTERAS duplicadas

df.dropDuplicates("id", "color").show // Elimina duplicados en base a las columnas id y color

df.dropDuplicates().show