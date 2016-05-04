package org.centrality.spark

import org.apache.spark.graphx.{EdgeRDD, GraphLoader, VertexRDD,lib}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
/**
  * Created by Ilias Sarantopoulos on 5/4/16.
  */
object ClosenessCentrality {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val graph = GraphLoader.edgeListFile(sc, "twitter_edges.txt")
    //val graph = GraphLoader.edgeListFile(sc, "followers.txt")
    //create a new RDD with just VertexId to be used for shortest paths
    // algorithm
    val vertexSeq = graph.vertices.map(v => v._1).collect().toSeq
    //println(vertexSeq)
    val spgraph = lib.ShortestPaths.run(graph, vertexSeq)
    val closeness = spgraph.vertices.map(vertex => (vertex._1 , 1f/vertex._2
      .values.sum))
    //sort vertices on descending degree value
    val sorted = closeness.sortBy(- _._2)
    //print top 10 vertices
    for ((vertexId, closeness) <- sorted.take(10)){
      println(s"Vertex with id ${vertexId} has a closeness degree of ${closeness}")
    }
    //spgraph.vertices.collect.foreach(v => println(v._1 , v._2))
    //closeness.collect.foreach(v => println(v))

  }

}
