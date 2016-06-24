package org.community.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, GraphLoader, GraphOps}
import org.apache.spark.{SparkConf, SparkContext, rdd}

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
    var graph = GraphLoader.edgeListFile(sc, "followers-new.txt")
    //initialization: assign each vertice in its own community
    var initial = graph.mapVertices((id, attr) => id.toInt)
    graph.collectNeighborIds(EdgeDirection.In).filter(x => x._1 == 3).foreach(x => println(s"vertex with id ${x._1} " +
      s"has " +
      s"neighbours ${x._2.deep.toString()}"))
    //initial.vertices.collect.foreach(println)
    var iter = 4
    do {
      for (vertex <- initial.vertices.collect()) {
        //get the array of the neighbours for the specific vertex
        var dQs: rdd.RDD[(Long, Long)] = sc.emptyRDD
        //TODO in/out edges or Either???
        var neighbours = graph.collectNeighborIds(EdgeDirection.Out).filter(x => x._1 == vertex._1).map(_._2).first()
        /*neighbours.collect.foreach(x => println(s"vertex with id ${x._1} " +
          s"has " +
          s"neighbours ${x._2.deep.toString()}"))*/
        //TODO calculate modularity for the vertexid itself
        for (neighbour <- neighbours) {
          //println(s"vertex with id ${vertex._1} has neighbour ${neighbour}")
          //calculate modularity if assignment is changed
          var dq = Array((neighbour, 2.toLong)) //modularity calculation for assignment to neighbor
          //create RDD with the new dq
          var newRDD = sc.parallelize(dq)
          //union with the initial dQ RDD
          dQs = dQs.union(newRDD)
        }
        dQs.collect.foreach(x => println(s" vid: ${x._1} has value ${x._2}"))
        var assignment = dQs.collect.maxBy(_._2)._1.toInt //TODO try and catch exception java.lang
        // .UnsupportedOperationException: empty.maxBy
        println(s"max is ${assignment}")
        graph = graph.mapVertices((id, attr) => if (id == vertex._1) assignment else attr)
      }
      graph.vertices.collect.foreach(println(_))
      //TODO create new graph by aggregating vertices and edges
      iter -= 1
      }while(iter != 0)
      //print time elapsed
      val time1 = System.currentTimeMillis()
      println(s"Executed in ${(time1 - time0) / 1000.0} seconds")

    }

}
