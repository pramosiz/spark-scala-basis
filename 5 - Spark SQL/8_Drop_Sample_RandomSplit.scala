// Funciones drop, sample y randomSplit

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// drop

df.printSchema

// Eliminamos las columnas comments_disabled, ratings_disabled y video_error_or_removed
val dfReducido = df.drop("comments_disabled", "ratings_disabled", "video_error_or_removed")

dfReducido.printSchema

// sample

// selecciona 90% de los datos. Los datos seleccionados son aleatorios
val dfSample = df.sample(0.9) 

val numFilas = df.count

val numSample = dfSample.count

// selecciona 90% de los datos con semilla 1234. Para que el resultado sea el mismo siempre
val dfSampleSeed = df.sample(0.9, 1234) 

dfSampleSeed.count

// selecciona 90% de los datos con reemplazo, es decir, puede haber filas repetidas
val dfSampleReplace = df.sample(true, 0.9, 1234)

dfSampleReplace.count

// randomSplit (para entrenar modelos de machine learning)

val Array(train, test) = df.randomSplit(Array(0.9, 0.1))

println(df.count)

println(train.count)

println(test.count)