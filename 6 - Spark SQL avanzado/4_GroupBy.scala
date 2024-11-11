// Agregación con agrupación

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

import org.apache.spark.sql.functions.{desc, asc, col, avg}

vuelos.groupBy("ORIGIN_AIRPORT") // Agrupamos por aeropuerto de origen
        .count                   // Contamos los vuelos
        .orderBy(desc("count"))  // Ordenamos por número de vuelos en orden descendente
        .show

vuelos.groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
        .count
        .orderBy(asc("count")) // Ordenamos por número de vuelos en orden ascendente
        .show
        