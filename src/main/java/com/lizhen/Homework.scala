package com.lizhen

import com.lizhen.nginx.parser.NginxParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object Homework {
  def main(args:Array[String]) = {
    val conf = new SparkConf().setAppName("test-es")
    val duration = 5
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "localhost:9500")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(duration))
    ssc.checkpoint("file///tmp/mapWithState/")

    val dstream = new TestInputStream[String](ssc, Mock.items2, duration)

    val result = dstream.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      val element = items(0).split("/")
      (
        List(items(2).split("/")(2) , element(0).split("-").mkString("/"), element(1).split(":")(0)).mkString("/"),
        nginxLogLine
      )
    }

//    val result2 = dstream.map { nginxLogLine =>
//      val items = NginxParser.parse(nginxLogLine)
//      val element = items(0).split("/")
//      (
//        items(2).split("/")(2)
//      )
//    }
//
//    result2.map( f => (f, 1)).reduceByKey( (a, b) => a + b ).print
//    result2.map( f => (f, List(f))).reduceByKey( (a,b) => a ++ b).print


    result.foreachRDD((rdd, durationTime) => {
      val domain = rdd.map(f => f._1.split("/")(0)).distinct().collect()
      println("******" + domain.mkString("/"))

      val domains = rdd.map( f => (f._1.split("/")(0), 1) ).reduceByKey( (a,b) => a + b)
      domains.foreach( f => println(f._1 + "*******" + f._2))



      val paths = rdd.map(f => f._1).distinct().collect()
      val pathToIndex = rdd.context.broadcast(paths.zipWithIndex.toMap)
      val indexToPath = rdd.context.broadcast(paths.zipWithIndex.map(_.swap).toMap)

      rdd.partitionBy( new Partitioner {
        override def numPartitions: Int = pathToIndex.value.size

        override def getPartition(key: Any): Int = {
          pathToIndex.value.get(key.toString).get
        }
      } ).mapPartitionsWithIndex((index, data) => {
        val dir = s"""file://tmp/${indexToPath.value(index)}"""
        val fileName = durationTime.milliseconds + "_" + index

        println("*****" + dir)
        println("*****" + fileName)
//        println(">>>>>" + indexToDomain.value(index))

//        savePartition(dir, fileName, data)
        data
      }, true).count()

//      EsSpark.saveToEs(rdd, "lizhen/test")
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def savePartition(path: String, fileName: String, iterator: Iterator[(String, String)]) = {
    var dos: FSDataOutputStream = null

    try {
      val wholePath = path + "/" + fileName
      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
      dos = fs.create(new Path(wholePath), true)
      iterator.foreach {
        line =>
          dos.writeBytes(line._2 + "\n")
      }
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      }
    }
  }
}
