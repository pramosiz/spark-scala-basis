// Función flatMap

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1,2,3,4,5))

rdd.collect

val rddCuadrado = rdd.map(x => List(x, x * x)) // List(1, 1), List(2, 4), List(3, 9), List(4, 16), List(5, 25)

rddCuadrado.collect

val rddCuadradoFlat = rdd.flatMap(x => List(x, x * x)) // 1, 1, 2, 4, 3, 9, 4, 16, 5, 25 -> Aplana la colección

rddCuadradoFlat.collect

val rddTexto = sc.parallelize(Seq("azul rojo verde", "morado amarillo negro"))

val rddColores = rddTexto.flatMap(_.split(" ")) // azul, rojo, verde, morado, amarillo, negro

rddColores.collect