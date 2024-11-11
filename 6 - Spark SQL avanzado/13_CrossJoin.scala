// Cross-Join

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

// Comportamiento peligroso
empleados.crossJoin(departamentos).show(100, false)