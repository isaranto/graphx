package org.centrality.spark
import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * Created by lias on 4/27/16.
  */
object MyGraph {

  def calcDegree() : Map[Long,Int] = {
      var degrees:Map[Long,Int] = Map()
      //degrees += (15 -> 1)
      return degrees

   }

  def main(args: Array[String]) {
    //val distFile = "/home/lias/twitter.edges"
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //val newRDD = sc.textFile("twitter_edges");
    //val distFile = sc.textFile(edgesFile())
    val graph = GraphLoader.edgeListFile(sc, "twitter_edges")
    var degrees:Map[Long,Int] = Map()
    for (triplet <- graph.triplets.collect) {
        println(s"${triplet.srcId} likes ${triplet.dstId}")}


  }


}
