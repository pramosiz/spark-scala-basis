// Función repartition. Repartition y Coalesce son operaciones muy costosas, 
// ya que requieren mover datos a través de la red para equilibrar las particiones. 
// Por lo tanto, se deben usar con precaución.

val sc = spark.sparkContext

val rdd = sc.parallelize((1 to 10), 5)

rdd.getNumPartitions

val rdd7 = rdd.repartition(7) // Aumenta el número de particiones

rdd7.getNumPartitions

val rdd3 = rdd.repartition(3)

rdd3.getNumPartitions