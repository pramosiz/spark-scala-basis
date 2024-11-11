import org.apache.spark.sql.SparkSession

object Main extends App{

    val sparkS = SparkSession.builder.appName("curso-scala-spark").getOrCreate()
    sparkS.conf.getAll.foreach(println)
}

