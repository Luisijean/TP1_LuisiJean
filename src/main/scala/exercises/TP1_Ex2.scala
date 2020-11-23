package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TP1_Ex2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/films.csv")

    val re_df: DataFrame = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    val films_leo: DataFrame = re_df.filter(re_df("acteur_principal") === "Di Caprio")
    print("Il y a " + films_leo.count() + " films de Leonardo Di Caprio")

    val moy_notes_leo: DataFrame = films_leo.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moy_notes_leo.show

    val vues_films  = re_df.agg(sum("nombre_vues")).first.get(0).toString.toDouble
    val vues_films_leo = films_leo.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val vues_leo_perc: Double = vues_films_leo / vues_films * 100
    print("Le pourcentage des vues des films de Di Caprio est de " + vues_leo_perc)

    val moy_notes_par_acteur = re_df.groupBy( col1 = "acteur_principal").mean( colNames = "nombre_vues")
    moy_notes_par_acteur.show


    val moy_vues_par_acteur = re_df.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moy_vues_par_acteur.show

    val vues_perc = re_df.withColumn(colName = "pourcentage_de_vues", col(colName = "nombre_vues") / vues_films * 100)
    vues_perc.show

  }

}
