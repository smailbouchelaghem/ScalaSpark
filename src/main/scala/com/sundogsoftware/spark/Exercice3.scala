package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object Exercice3 {

case class OrderbyC(userId : Int, itemId : Int, Amount : Float)

  def parseddata(line : String):(String, String) ={
    val fields = line.split(",")
    (fields(0),fields(2))


  }

  def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession
    .builder
    .appName("Exercice3")
    .master("local[*]")
    .getOrCreate()

  val inputSchema = new StructType()
    .add("userId", IntegerType, nullable = true)
    .add("itemId", IntegerType, nullable = true)
    .add("amount", FloatType, nullable = true)

  import spark.implicits._

  val input = spark.read
    .schema(inputSchema)
    .csv("data/customer-orders.csv")
    .as[OrderbyC]

    input.show()

  val id_amount = input.select("userId", "Amount").groupBy("userId").sum("amount")

    val id_amount_withculumn = id_amount.withColumn("Total Spent", functions.round($"sum(amount)", 2)).select("userId", "Total Spent").orderBy("Total Spent")


    val result = id_amount_withculumn.collect()
    id_amount_withculumn.show(result.length.toInt)
  }

}
