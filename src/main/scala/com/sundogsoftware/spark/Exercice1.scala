package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Exercice1 {

    def parsedata(line :String) : (Int, Float) = {
      val data1 = line.split(",")
      val cunstumerID = data1(0).toInt
      val price = data1(2).toFloat
      (cunstumerID, price)
    }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "Excercie1")

    val input = sc.textFile("data/customer-orders.csv")


    val countprice = input.map(parsedata).reduceByKey((x,y)=> x+y)

      countprice.foreach(println)

    val result = countprice.collect().sortBy(_._2)

    for(re <- result) {
      val Id = re._1
      val amount = re._2

      println(s"$Id : $amount" )
    }



  }

}
