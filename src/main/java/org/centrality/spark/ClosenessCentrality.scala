package org.centrality.spark

import org.apache.spark.graphx.{EdgeRDD, GraphLoader, VertexRDD,lib}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
/**
  * Created by Ilias Sarantopoulos on 5/4/16.
  * Closeness centrality is calculated for each vertex as follows:
  * d(u) = 1 / Sum_of_shortest_paths_from_u_to_all_other_vertices
  */
object ClosenessCentrality {
  def main(args: Array[String]) {
    val time0 = System.currentTimeMillis()
    //System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Closeness")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    //var graph = GraphLoader.edgeListFile(sc, "hdfs://sparkmaster:9000/user/ilias/followers-new.txt")
    var graph = GraphLoader.edgeListFile(sc, "hdfs://sparkmaster:9000/user/ilias/twitter_edges.txt")
    //create a new RDD with just VertexId to be used for shortest paths
    // algorithm
    val vertexSeq = graph.vertices.map(v => v._1).collect().toSeq
    //println(vertexSeq)
    val spgraph = lib.ShortestPaths.run(graph, vertexSeq)
    val closeness = spgraph.vertices.map(vertex => (vertex._1 , 1f/vertex._2
      .values.sum))
    //sort vertices on descending degree value
    val sorted = closeness.sortBy(- _._2)
    //print time elapsed
    val time1 = System.currentTimeMillis()
    println(s"Executed in ${(time1-time0)/1000.0} seconds")
    //print top 10 vertices
    for ((vertexId, closeness) <- sorted.take(10)){
      println(s"Vertex with id ${vertexId} has a closeness degree of ${closeness}")
    }
    //spgraph.vertices.collect.foreach(v => println(v._1 , v._2))
    //closeness.collect.foreach(v => println(v))

  }

}
