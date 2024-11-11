// Varias agregaciones por grupo

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

import org.apache.spark.sql.functions.{count, min, max, desc, avg}

vuelos.groupBy("ORIGIN_AIRPORT").agg( // Agregaciones por aeropuerto de origen
  count("AIR_TIME").as("tiempo_aire"), // Añade una columna con el conteo de vuelos
  min("AIR_TIME").as("min"),          // Añade una columna con el menor tiempo de vuelo
  max("AIR_TIME").as("max")           // Añade una columna con el mayor tiempo de vuelo
).orderBy(desc("tiempo_aire")).show   // Ordena por el conteo de vuelos

vuelos.groupBy("MONTH").agg(
  count("ARRIVAL_DELAY").as("conteo_retrasos"),
  avg("DISTANCE").as("promedio_distancia")
).orderBy(desc("conteo_retrasos")).show
