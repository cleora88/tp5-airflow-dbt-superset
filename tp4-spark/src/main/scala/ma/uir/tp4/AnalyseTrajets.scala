package ma.uir.tp4

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

// Deux classes pour l'API typée (Dataset) : une ligne lue, une ligne produite
case class LongTrajet(conducteur: Int, ville: String, distance_km: Double,
                      conso_reelle: Double)
case class BilanConducteur(conducteur: Int, nb_longs_trajets: Int, km: Double,
                           conso_moyenne: Double)

/** Du bronze (trajets en Parquet) aux tables Gold.
  * Arguments facultatifs, sous la forme clé=valeur :
  *   entree=s3a://bronze/trajets/   depuis=2025-01   cache=non
  */
object AnalyseTrajets {

  // Le schéma attendu, déclaré plutôt que deviné
  val SchemaTrajets: StructType = StructType(Seq(
    StructField("id_trajet", LongType),
    StructField("id_voiture", StringType),
    StructField("conducteur", IntegerType),
    StructField("ville", StringType),
    StructField("debut", TimestampType),
    StructField("type_route", StringType),
    StructField("distance_km", DoubleType),
    StructField("duree_min", IntegerType),
    StructField("carburant_l", DoubleType),
    StructField("mois", StringType)))

  /** Exécute un bloc, lui donne un nom dans la Spark UI et affiche sa durée. */
  def etape[T](nom: String)(bloc: => T)(implicit spark: SparkSession): T = {
    spark.sparkContext.setJobDescription(nom)
    val t0 = System.nanoTime()
    val resultat = bloc
    println(f"$nom%-44s ${(System.nanoTime() - t0) / 1e9}%6.1f s")
    resultat
  }

  def main(args: Array[String]): Unit = {
    val options = args.map(_.split("=", 2)).collect { case Array(c, v) => c -> v }.toMap
    val entree    = options.getOrElse("entree", "s3a://bronze/trajets/")
    val depuis    = options.getOrElse("depuis", "2025-01")
    val avecCache = options.getOrElse("cache", "non") == "oui"

    // Ni master ni nom d'application dans le code : spark-submit les fournit
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val t0 = System.nanoTime()
    println(s"Options : entree=$entree depuis=$depuis cache=$avecCache")

    // ---- 1. Lire ----------------------------------------------------------------
    val brut = spark.read.schema(SchemaTrajets).parquet(entree)
      .filter($"mois" >= depuis)              // colonne de partition : dossiers ignorés

    // ---- 2. Nettoyer ------------------------------------------------------------
    val propres = brut
      .filter($"carburant_l".isNotNull && $"carburant_l" > 0)  // capteurs en panne
      .where("distance_km BETWEEN 1 AND 1000")                 // filtre écrit en SQL
      .dropDuplicates("id_trajet")                             // trajets en double
      .withColumn("ville", initcap(trim($"ville")))            // "  RABAT" : "Rabat"
      .na.fill("Inconnue", Seq("ville"))
      .withColumn("conso_reelle", round($"carburant_l" / $"distance_km" * 100, 2))
      .withColumn("vitesse_kmh", round($"distance_km" / $"duree_min" * 60, 1))
      .withColumn("moment",
        when(hour($"debut") < 6, "nuit").when(hour($"debut") < 12, "matin")
          .when(hour($"debut") < 18, "après-midi").otherwise("soir"))
      .drop("duree_min", "carburant_l")

    // ---- 3. Joindre au référentiel (table Iceberg du TP3) -----------------------
    val voitures = spark.table("lake.silver.voitures")
      .select($"id_voiture", $"nom", $"annee", $"origine",
        $"conso_l100".as("conso_annoncee"))

    val enrichis = propres
      .join(voitures, Seq("id_voiture"), "inner")
      .withColumn("ecart_pct",
        round(($"conso_reelle" - $"conso_annoncee") / $"conso_annoncee" * 100, 1))

    if (avecCache) enrichis.persist(StorageLevel.MEMORY_AND_DISK)
    val nbTrajets = etape("1-3 lire, nettoyer, joindre (count)") { enrichis.count() }
    println(s"Trajets valides : $nbTrajets")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

    // ---- 4. Agréger : groupBy et agg --------------------------------------------
    etape("4 consommation réelle par modèle") {
      enrichis
        .groupBy("nom", "annee", "origine")
        .agg(
          count(lit(1)).as("nb_trajets"),
          approx_count_distinct("conducteur").as("nb_conducteurs"),
          round(sum("distance_km")).cast("long").as("km"),
          first("conso_annoncee").as("conso_annoncee"),
          round(avg("conso_reelle"), 2).as("conso_reelle"),
          round(avg("ecart_pct"), 1).as("ecart_pct"),
          round(max("vitesse_kmh"), 1).as("vitesse_max"))
        .filter($"nb_trajets" >= 1000)                // l'équivalent du HAVING de SQL
        .orderBy(desc("ecart_pct"))
        .writeTo("lake.gold.conso_reelle_par_modele").using("iceberg").createOrReplace()
    }

    // ---- 5. Tableau croisé : pivot ----------------------------------------------
    etape("5 kilomètres par ville et type de route") {
      enrichis
        .groupBy("ville")
        .pivot("type_route", Seq("urbain", "route", "autoroute"))  // 3 colonnes
        .agg(round(sum("distance_km")).cast("long"))
        .withColumnRenamed("urbain", "km_urbain")
        .withColumnRenamed("route", "km_route")
        .withColumnRenamed("autoroute", "km_autoroute")
        .withColumn("km_total", $"km_urbain" + $"km_route" + $"km_autoroute")
        .orderBy($"km_total".desc)
        .writeTo("lake.gold.km_par_ville").using("iceberg").createOrReplace()
    }

    // ---- 6. Fenêtres : classement, mois précédent, cumul ------------------------
    etape("6a trois modèles les plus utilisés par ville") {
      val parVille = Window.partitionBy("ville").orderBy(desc("km"))
      enrichis
        .groupBy("ville", "nom", "annee")
        .agg(round(sum("distance_km")).cast("long").as("km"))
        .withColumn("rang", dense_rank().over(parVille))
        .filter($"rang" <= 3)
        .writeTo("lake.gold.top_modeles_par_ville").using("iceberg").createOrReplace()
    }

    etape("6b activité mensuelle et évolution") {
      val chrono = Window.partitionBy("origine").orderBy("mois")
      val cumul  = chrono.rowsBetween(Window.unboundedPreceding, Window.currentRow)
      enrichis
        .groupBy("origine", "mois")
        .agg(count(lit(1)).as("nb_trajets"),
          round(sum("distance_km")).cast("long").as("km"))
        .withColumn("km_mois_precedent", lag("km", 1).over(chrono))
        .withColumn("evolution_pct",
          round(($"km" - $"km_mois_precedent") / $"km_mois_precedent" * 100, 1))
        .withColumn("km_cumules", sum("km").over(cumul))
        .writeTo("lake.gold.activite_mensuelle").using("iceberg")
        .partitionedBy($"origine").createOrReplace()
    }

    // ---- 7. Fenêtre sur toutes les lignes : rotation des voitures ---------------
    etape("7 rotation des voitures (fenêtre)") {
      val parVoiture = Window.partitionBy("id_voiture").orderBy("debut")
      enrichis
        .withColumn("precedent", lag("debut", 1).over(parVoiture))
        .withColumn("attente_h",
          (unix_timestamp($"debut") - unix_timestamp($"precedent")) / 3600)
        .groupBy("id_voiture", "nom", "annee")
        .agg(count(lit(1)).as("nb_trajets"),
          round(avg("attente_h"), 2).as("attente_moyenne_h"),
          round(max("attente_h"), 1).as("attente_max_h"))
        .writeTo("lake.gold.rotation_par_voiture").using("iceberg").createOrReplace()
    }


    // ---- 8. API typée (Dataset), puis RDD ---------------------------------------
    val longs: Dataset[LongTrajet] = enrichis
      .select("conducteur", "ville", "distance_km", "conso_reelle")
      .as[LongTrajet]
      .filter(t => t.distance_km > 400)       // une fonction Scala, typée

    etape("8a bilan des conducteurs (Dataset)") {
      longs
        .groupByKey(_.conducteur)
        .mapGroups { (conducteur, trajets) =>
          val liste = trajets.toSeq
          BilanConducteur(conducteur, liste.size, liste.map(_.distance_km).sum,
            liste.map(_.conso_reelle).sum / liste.size)
        }
        .filter(_.nb_longs_trajets >= 5)
        .toDF()
        .writeTo("lake.gold.bilan_conducteurs").using("iceberg").createOrReplace()
    }

    etape("8b longs trajets par ville (RDD)") {
      longs.rdd
        .map(t => (t.ville, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .collect()
        .foreach { case (ville, nb) => println(f"   $ville%-12s $nb%7d") }
    }

    // ---- 9. Fonction utilisateur (UDF) et Spark SQL -----------------------------
    val profil = udf((ecart: Double) =>
      if (ecart > 20) "gourmand" else if (ecart < 0) "sobre" else "conforme")
    spark.udf.register("profil", profil)
    enrichis.createOrReplaceTempView("trajets")

    etape("9 profils de conduite (UDF et SQL)") {
      spark.sql("""
        SELECT origine, profil(ecart_pct) AS profil, COUNT(*) AS nb_trajets,
               ROUND(AVG(conso_reelle), 2) AS conso_moyenne,
               ROUND(AVG(vitesse_kmh), 1) AS vitesse_moyenne
        FROM trajets
        WHERE moment <> 'nuit'
        GROUP BY origine, profil(ecart_pct)
        ORDER BY origine, nb_trajets DESC""").show(truncate = false)
    }

    // ---- 10. Exporter : un seul fichier CSV -------------------------------------
    etape("10 export CSV pour le tableur") {
      spark.table("lake.gold.conso_reelle_par_modele")
        .coalesce(1)
        .write.mode("overwrite").option("header", "true")
        .csv("s3a://lakehouse/exports/conso_reelle_par_modele")
    }


    println(f"Durée totale : ${(System.nanoTime() - t0) / 1e9}%.1f s")
    spark.stop()
  }
}
