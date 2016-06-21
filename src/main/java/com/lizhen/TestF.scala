package com.lizhen

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
  * Created by easyin on 6/15/16.
  */
object TastF {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test-stuq")

    val ssc = new StreamingContext(conf, Seconds(30))

    val inputStream = new TestInputStream[String](ssc, Mock.items, 1)

    val tempDstream = inputStream.map(f => (f,1))
  //val tempDstream = inputStream.map(f => (f,1)).reduceByKey((a,b) => (0))
    val lineLength = inputStream.map(s => s.length)
    val totalLength = lineLength.reduce((a,b) => a + b)

    println("**********")
    println(totalLength)

    tempDstream.foreachRDD{
      rdd => rdd.foreach(
        f => {
          println(f)
        }
      )
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

