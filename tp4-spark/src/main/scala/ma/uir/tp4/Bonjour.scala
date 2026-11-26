package ma.uir.tp4

import org.apache.spark.sql.SparkSession

/** Premier programme : vérifie que le JAR se lance et dit sur quoi il tourne. */
object Bonjour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Bonjour").getOrCreate()
    val sc = spark.sparkContext

    // Un calcul distribué minuscule : la somme des entiers de 0 à 999 999
    spark.range(1000000).selectExpr("sum(id) AS somme").show()
    println(s"Spark ${spark.version} sur ${sc.master}, ${sc.defaultParallelism} coeurs")

    spark.stop()
  }
}