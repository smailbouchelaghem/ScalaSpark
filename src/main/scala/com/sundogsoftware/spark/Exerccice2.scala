package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Exerccice2 {

  case class friend(id : Int ,name : String ,age : Int ,friends : Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder
      .appName("Exercice2")
      .master("local[*]")
      .getOrCreate()

    import ss.implicits._

    val inputdata = ss.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[friend]


    println("Here is our inferred schema:")
    inputdata.printSchema()

    val agefriends = inputdata.select("age","friends")

    agefriends.groupBy("age")
      .agg(round(avg("friends"),2).alias("avg_of_friends"))
      .sort("age")
      .show()

    ss.stop()


  }

}
