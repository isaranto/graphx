package org.centrality.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{GraphLoader, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ilias Sarantopoulos on 5/4/16.
  */
object EigenVectorCentrality {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val graph = GraphLoader.edgeListFile(sc, "followers-new.txt")
    val nodeNumber = graph.numVertices
    val previousValue = graph.mapVertices((vId, eigenvalue) => 1.0 / nodeNumber)
    val zeroValue = graph.mapVertices((vId, eigenvalue) => 0.0)
    val iter = 4
    var oldValue = previousValue
    var newVertices = previousValue.vertices
    for ( x <- 1 to iter){
        //previousValue.vertices.sortBy(_._1).collect.foreach(v => println(v._1 , v._2))
        //previousValue.vertices.collect.foreach(v => println(v._2))
        val outDegrees: VertexRDD[Int] = graph.outDegrees
        if (x>=1) {oldValue = zeroValue}
        var rankGraph = oldValue
          .outerJoinVertices(graph.outDegrees) { (vid, eigenvalue, deg) => deg.getOrElse(0)}
          // Set the weight on the edges based on the outdegree
          .mapTriplets(e => 1.0 / e.srcAttr)
          .outerJoinVertices(newVertices) { (vid, deg, eigenValue) => eigenValue.getOrElse(0.0) }
        newVertices = rankGraph.aggregateMessages[(Double)](
            triplet => { // Map Function
                  //calculate how much each vertex "contributes" to the destination vertex
                  triplet.sendToDst(triplet.srcAttr*triplet.attr)
                },
                // Add all vertices old eigenvalues of inVertices to sum the eigenvalue of each vertex
                (a, b) => (a + b) // Reduce Function
        )
        rankGraph = rankGraph.outerJoinVertices(newVertices){ (vid, oldvalue, newvalue) => newvalue.getOrElse(0) }
      if (x==iter){
        //after the last iteration print the top 10 eigenvector centrality values and the attributes of the edges
        rankGraph.vertices.sortBy(-_._2).take(10).foreach(v => println(v))
        rankGraph.edges.collect.foreach{println(_)}
      }
    }
  }
}
