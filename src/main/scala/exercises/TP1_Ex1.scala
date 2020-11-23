package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TP1_Ex1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data/films.csv")
    //Question 2
    val films_leo = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a " + films_leo.count() + " films de Leonardo Di Caprio"+"\n")
    //Question 3
    val films_grade_leo = films_leo.map(item => (item.split(";")(2).toDouble) )
    val moyenne = films_grade_leo.sum
    print("La moyenne des films de Di Caprio est: " + moyenne/films_leo.count()+ "\n")
    //Question 4
    val views_leo = films_leo.map(item => (item.split(";")(1).toDouble))
    val allViews = rdd.map(item => (item.split(";")(1).toDouble))
    val rate_views_leo = views_leo.sum() / allViews.sum()
    println("Le pourcentage des vues des films de Di Caprio: "+rate_views_leo*100+"%")
    //Question 5
    val acteurs = rdd.map(item => (item.split(";")(3))).distinct.collect()
    acteurs.foreach(println)

  }

}
