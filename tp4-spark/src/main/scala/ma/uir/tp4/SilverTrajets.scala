package ma.uir.tp4

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Nettoie les trajets d'UNE journée du bronze et les range dans lake.silver.trajets.
  * Argument : la journée à traiter (AAAA-MM-JJ), fournie par Airflow ({{ ds }}).
  * Idempotent : relancé pour la même journée, il remplace la partition, sans doublon.
  */
object SilverTrajets {

  def main(args: Array[String]): Unit = {
    val jour = args(0)
    val mois = jour.substring(0, 7)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // 1. Le bronze du mois (dossier mois=AAAA-MM du TP4), réduit à la journée demandée
    val brut = spark.read.parquet(s"s3a://bronze/trajets/mois=$mois/")
      .filter(to_date($"debut") === to_date(lit(jour)))

    // 2. Nettoyage : un trajet reçu deux fois est gardé une fois ; villes normalisées
    val nettoye = brut
      .dropDuplicates("id_trajet")
      .withColumn("ville", initcap(trim($"ville")))
      .withColumn("jour", to_date($"debut"))
      .withColumn("ingere_a", current_timestamp())   // pour la fraîcheur (dbt)

    // 3. Les lignes inutilisables partent dans une table de rejets, avec leur motif
    val valide = $"ville".isNotNull && $"carburant_l".isNotNull && $"carburant_l" > 0
    val trajets = nettoye.filter(valide)
    val rejets = nettoye.filter(!valide).withColumn("motif",
      when($"ville".isNull, "ville absente")
        .when($"carburant_l".isNull, "carburant absent")
        .otherwise("carburant négatif"))

    // 4. Écriture idempotente : la partition de la journée est remplacée, pas complétée
    ecrire(spark, trajets, "lake.silver.trajets")
    ecrire(spark, rejets, "lake.silver.trajets_rejets")

    val (n, r) = (trajets.count(), rejets.count())
    println(f"Silver $jour : $n%d trajets valides, $r%d rejets")
    spark.stop()
  }

  /** Crée la table partitionnée par jour, puis remplace la partition ensuite. */
  def ecrire(spark: SparkSession, df: DataFrame, table: String): Unit =
    if (spark.catalog.tableExists(table)) df.writeTo(table).overwritePartitions()
    else df.writeTo(table).using("iceberg").partitionedBy(col("jour")).create()
}
