package org.community.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lias on 6/21/16.
  */
object LouvainModularity {
  def main(args: Array[String]) {
    val time0 = System.currentTimeMillis()
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val graph = GraphLoader.edgeListFile(sc, "followers-new.txt")


    //print time elapsed 
    val time1 = System.currentTimeMillis()
    println(s"Executed in ${(time1-time0)/1000.0} seconds")
  }
}
