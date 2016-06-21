package com.lizhen

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object ESTest {

  def main(args : Array[String]) :Unit = {
    val conf = new SparkConf().setAppName("ESTest").setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "localhost:9500")
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" ->3)
    val airports = Map("arrival" -> "Otopn", "sfo" -> "san fro")

    val rdd = sc.makeRDD(Seq(numbers, airports))
    EsSpark.saveToEs(rdd, "spark/docs")
  }

}
