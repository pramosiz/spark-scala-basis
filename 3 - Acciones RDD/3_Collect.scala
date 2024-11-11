// Función collect

val sc = spark.sparkContext

val rdd = sc.parallelize("Hola Spark con Databricks!".split(" "))

rdd.collect // Recopila todos los elementos del RDD en 
// un array y lo devuelve al controlador (driver)

val arrayString = rdd.collect()

arrayString(1)

arrayString(0)

val rddNumero = sc.parallelize(1 to 10).flatMap(x => List(x, x*x))

rddNumero.collect