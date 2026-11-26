package ma.uir.tp4

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/** Génère les trajets des voitures de lake.silver.voitures, en Parquet, dans le bronze.
  * Arguments : [nombre de trajets] [dossier de sortie]
  */
object GenererTrajets {

  val Villes = Seq("Rabat", "Casablanca", "Tanger", "Fès", "Marrakech", "Agadir",
    "Oujda", "Meknès")

  /** Un nombre entre 0 et 1 calculé à partir de l'identifiant du trajet : le même
    * sur tous les postes, quel que soit le découpage en partitions. */
  def alea(graine: Int): Column =
    pmod(xxhash64(col("id_trajet"), lit(graine)), lit(1000000)) / 1000000.0

  def main(args: Array[String]): Unit = {
    val nbTrajets = if (args.length > 0) args(0).toLong else 5000000L
    val sortie    = if (args.length > 1) args(1) else "s3a://bronze/trajets/"

    val spark = SparkSession.builder().appName("GenererTrajets").getOrCreate()
    import spark.implicits._

    // 1. Le référentiel : les voitures de la table Silver du TP3, numérotées de 0 à n-1
    val voitures = spark.table("lake.silver.voitures")
      .select("id_voiture", "conso_l100")
      .orderBy("id_voiture")
      .rdd.zipWithIndex()                        // un RDD de couples (ligne, numéro)
      .map { case (ligne, n) => (n.toInt, ligne.getString(0), ligne.getDouble(1)) }
      .toDF("numero", "id_voiture", "conso_l100")
      // Chaque voiture s'écarte à sa façon de la consommation annoncée (-5 % à +24 %)
      .withColumn("biais", lit(0.95) + pmod(xxhash64($"id_voiture"), lit(30)) / 100.0)
    val nbVoitures = voitures.count()
    println(s"Voitures du référentiel : $nbVoitures")

    // 2. Un trajet par ligne : spark.range fabrique la colonne id sur les exécuteurs
    val urbain = col("type_route") === "urbain"
    val route  = col("type_route") === "route"
    val trajets = spark.range(nbTrajets).withColumnRenamed("id", "id_trajet")
      // Données déséquilibrées : 30 % des trajets sont faits par la voiture numéro 0
      .withColumn("numero",
        when(alea(1) < 0.30, 0).otherwise(floor(alea(2) * nbVoitures)).cast("int"))
      .withColumn("ville",
        element_at(array(Villes.map(lit): _*), (floor(alea(3) * 8) + 1).cast("int")))
      .withColumn("debut",
        timestamp_seconds(lit(1735689600L) + floor(alea(4) * 365 * 86400)))
      .withColumn("type_route",
        when(alea(5) < 0.5, "urbain").when(alea(5) < 0.8, "route")
          .otherwise("autoroute"))
      .withColumn("distance_km", round(
        when(urbain, lit(2) + alea(6) * 18).when(route, lit(10) + alea(6) * 100)
          .otherwise(lit(50) + alea(6) * 400), 1))
      .withColumn("duree_min",
        ceil($"distance_km" * 60 / when(urbain, 28).when(route, 70).otherwise(110))
          .cast("int"))
      .withColumn("conducteur", (floor(alea(7) * 20000) + 1).cast("int"))

    // 3. Le carburant dépend de la voiture : jointure avec le référentiel, diffusé
    //    (broadcast) à tous les exécuteurs
    val avecConso = trajets.join(broadcast(voitures), "numero")
      .withColumn("carburant_l", round(
        $"distance_km" * $"conso_l100" / 100 * $"biais"
          * when(urbain, 1.25).when(route, 1.0).otherwise(0.9)
          * (lit(0.9) + alea(8) * 0.2), 2))

    // 4. Des défauts, comme dans de vraies données : capteurs en panne, villes mal
    //    saisies, trajets reçus deux fois
    val sales = avecConso
      .withColumn("carburant_l",
        when(alea(9) < 0.010, lit(null)).when(alea(9) < 0.015, -$"carburant_l")
          .otherwise($"carburant_l"))
      .withColumn("ville",
        when(alea(10) < 0.02, concat(lit("  "), upper($"ville")))
          .when(alea(10) < 0.03, lit(null)).otherwise($"ville"))
      .withColumn("mois", date_format($"debut", "yyyy-MM"))
      .select("id_trajet", "id_voiture", "conducteur", "ville", "debut", "type_route",
        "distance_km", "duree_min", "carburant_l", "mois")
    val avecDoublons = sales.union(sales.filter(alea(11) < 0.01))

    // 5. Écrire en Parquet, un dossier par mois
    avecDoublons
      .repartition($"mois")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("mois")
      .parquet(sortie)

    println(s"Trajets écrits dans $sortie : ${spark.read.parquet(sortie).count()}")
    spark.stop()
  }
}
