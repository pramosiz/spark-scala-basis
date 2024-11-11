// Persistencia de DataFrames en la memoria.
// Spark SQL conoce la estructura de los datos del DataFrame,
// por lo que organiza los datos en un formato de columnas y aplica las
// compresiones necesarias para optimizar el almacenamiento en memoria.
// Como resultado, almacenar un DataFrame es más eficiente que almacenar un RDD,
// cuando ambos tienen la misma cantidad de datos.

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.persist  // Almacenamiento en memoria por defecto

df.unpersist // Eliminar el almacenamiento en memoria

df.cache // Almacenamiento en memoria por defecto

df.unpersist

import org.apache.spark.storage.StorageLevel

df.persist(StorageLevel.DISK_ONLY) // Almacenamiento en disco

df.unpersist

df.persist(StorageLevel.MEMORY_AND_DISK) // Almacenamiento en memoria y disco

df.count