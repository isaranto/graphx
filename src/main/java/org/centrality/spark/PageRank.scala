package org.centrality.spark
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
  * Created by Ilias Sarantopoulos on 5/6/16.
  * Running GraphX PageRank example from documentation
  * on my twitter network
  */
object PageRank {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("informationRetrieval2016").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "my_twitter_edges.txt")
    // Run PageRank on my twitter network
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("my_twitter_users.txt").map { line =>  val
    fields = line
      .split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    val sorted = ranksByUsername.sortBy(- _._2)
    //print top 10 vertices
    for ((vertexId, degree) <- sorted.take(10)){
      println(s"User: ${vertexId} has a pagerank degree of ${degree}")
    }

 }


}
