// Trabajo con datos incorrectos o faltantes

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.count

// 1. Borrado de datos faltantes
//borrando aquellas filas que por lo menos tengan un valor perdido
df.na.drop.count

//otra alternativa
df.na.drop("any").count

// borrar solo aquellas filas donde todas las columnas sean nulas
df.na.drop("all").count

// borra sola aquellas columnas donde el valor de la columna views es nulo
df.na.drop(Array("views")).count

// otra alternativa
df.na.drop(Seq("views", "dislikes")).count


// 2. Rellenado de datos faltantes 

// realizamos una consulta primeramente
import org.apache.spark.sql.functions.col
df.orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()

//rellenando los na con cero. Debemos tener en cuenta que esto rellenara con ceros TODAS las columnas de tipo integer y long
df.na.fill(0).orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()

// SOLO rellenar columnas seleccionadas
df.na.fill(0, Array("views", "likes")).orderBy(col("views")).select(col("views"), col("likes"), col("dislikes")).show()

// Analizando el panoramal general de una columna
df.describe("likes").show()
// Muestra datos como el total de registros, media, desviacion estandar, minimo, maximo
