package org.community.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lias on 6/21/16.
  */
object LouvainModularity {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val graph = GraphLoader.edgeListFile(sc, "followers-new.txt")

}
