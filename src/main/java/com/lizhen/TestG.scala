package com.lizhen

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by easyin on 6/16/16.
  */
object TestG {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test-stuq")

    val ssc = new StreamingContext(conf, Seconds(30))

    val lines = ssc.textFileStream("./a1.txt")
    val words = lines.flatMap(_.split(" "))

    println("==========")
    lines.print()
  }
}
