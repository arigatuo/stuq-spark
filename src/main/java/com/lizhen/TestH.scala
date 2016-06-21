package com.lizhen

import com.lizhen.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
  * Created by easyin on 6/17/16.
  */
object TestH {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("test-streaming-app")
    val isDebug = true
    val duration = 5

    if (isDebug) {
      conf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(conf, Seconds(duration))

//    val input = if (isDebug) new TestInputStream[String](ssc, Mock.items, 1)
//    else {
//
//    }
    val input = if (isDebug) new TestInputStream[String](ssc, Mock.items, 1)
    else {
      new TestInputStream[String](ssc, Mock.items, 1)
    }

    //Transform
    val result = input.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      items(2).split("/")(2)
    }

    result.foreachRDD { rdd =>
      rdd.foreach(line =>
        println("====" + line)
      )
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
