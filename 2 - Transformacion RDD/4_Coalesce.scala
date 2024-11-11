// Función coalesce

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1 to 10), 10)

rdd.getNumPartitions

val rdd5 = rdd.coalesce(5) // Reduce el número de particiones a 5

rdd5.getNumPartitions