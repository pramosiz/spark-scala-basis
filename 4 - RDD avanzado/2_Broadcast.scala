// Broadcast Variable

val sc = spark.sparkContext

val uno = 1

val brUno = sc.broadcast(uno)

brUno.value

brUno.value + 1

brUno.destroy // Se destruye el broadcast

brUno.value + 1 // ERROR. No se puede acceder a una variable broadcast destruida
