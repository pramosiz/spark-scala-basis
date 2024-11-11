// Función count

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq("j", "o", "s", "e"))

rdd.count // Cuenta el número de elementos en el RDD y lo envía al controlador (driver)

val rddN = sc.parallelize(1 to 50)

rddN.count