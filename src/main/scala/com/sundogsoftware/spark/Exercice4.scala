package com.sundogsoftware.spark

import com.sundogsoftware.spark.MostPopularSuperheroDataset.{SuperHero, SuperHeroNames}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Exercice4 {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MostObscureSuperhero")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopular = connections
      .sort($"connections")

    val filtredobscure = mostPopular.filter($"connections" === 1)

    val minConnectionWithName = filtredobscure.join(names,"id")



    println(s"those are the most obscured superhero with 1 co-appearances.")

    minConnectionWithName.select("name").show(minConnectionWithName.count.toInt)
  }




}
