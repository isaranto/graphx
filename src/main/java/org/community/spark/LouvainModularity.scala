package org.community.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
  * Created by lias on 6/21/16.
  */
object LouvainModularity{
  def main(args: Array[String]) {
    val time0 = System.currentTimeMillis()
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    var graph = GraphLoader.edgeListFile(sc, "followers-new.txt")
    var originalGraph = graph.mapVertices((id, attr) => Array(id.toInt))
    graph.collectNeighborIds(EdgeDirection.In).filter(x => x._1 == 3).foreach(x => println(s"vertex with id ${x._1} " +
      s"has " +
      s"neighbours ${x._2.deep.toString()}"))
    //initial.vertices.collect.foreach(println)
    // m is the m from the dQ function of modularity(total number of edges)
    var iter = 4
    do {
      //initialization: assign each vertice in its own community
      var initial = graph.mapVertices((id, attr) => id.toInt)
      //Phase 1 :Calculate change in modularity
      var m = graph.edges.collect.length/2
      for (vertex <- initial.vertices.collect()) {
        //get the array of the neighbours for the specific vertex(and append the id of itself)
        var dQs: rdd.RDD[(Long, Double)] = sc.emptyRDD
        var neighbours = graph.collectNeighborIds(EdgeDirection.Either).filter(x => x._1 == vertex._1).map(_._2)
          .first() ++ Array(vertex._1)
        //println(s"vertex with id ${vertex._1} has neighbours ${neighbours.deep.toString()}")
        var k_i = graph.degrees.collect.filter(x => x._1 == vertex._1).map(x => x._2).toSeq(0)
        for (neighbour <- neighbours) {
          //println(s"vertex with id ${vertex._1} has neighbour ${neighbour}")
          //calculate modularity if assignment is changed
          //var dq = Array((neighbour, 2.toLong)) //modularity calculation for assignment to neighbor
          //get graph.Vertex from id
          var n = graph.vertices.filter(x => x._1 == neighbour).first()
          var k_i_inC = graph.edges.filter(e => (e.dstId==vertex._1 || e.srcId ==vertex._1)&& (e.dstId==neighbour ||
            e.srcId ==neighbour)).count()
          var Sin = graph.edges.filter(e => (e.dstId==neighbour && e.srcId==neighbour)||(e.srcId==neighbour && e
            .dstId==neighbour)).count()
          var Stot = graph.edges.filter(e => e.dstId==neighbour).count()
          var dq = Array((neighbour,(((Sin+k_i_inC)/2*m)- math.pow((Stot+k_i)/2*m,2))-(Sin/2*m -math.pow(Stot/2*m,2) -
            math.pow(k_i/2*m,2))))
          //create RDD with the new dq
          var newRDD = sc.parallelize(dq)
          //union with the initial dQ RDD
          dQs = dQs.union(newRDD)
        }
        //dQs.collect.foreach(x => println(s" vid: ${x._1} has value ${x._2}"))
        //check if there exists a positive value. If not vertex will not change community
        var maxQ = dQs.collect.maxBy(_._2)._2
        var assignment  = if (maxQ > 0) dQs.collect.maxBy(_._2)._1.toInt else vertex._1.toInt
        //println(s"max is ${assignment}")
        graph = graph.mapVertices((id, attr) => if (id == vertex._1) assignment else attr)
      }
      //graph.vertices.collect.foreach(println(_))
      //Phase 2: Built new graph as "graph" and store communities as attributes on originalGraph
      var newVertices = graph.vertices.map(x => (x._2.toLong, x._2))
      var newEdges = graph.triplets.map(
        triplet =>
            (triplet.srcAttr,triplet.dstAttr, 1)
                ).collect()
      var edgesNew = newEdges.map(x => new Edge(x._1,x._2,x._3))
      newVertices.collect.foreach(println)
      graph = Graph(newVertices, sc.parallelize(edgesNew))
      //TODO save community structure in the original graph object
      iter -= 1
      }while(iter != 0)
      //print time elapsed
      val time1 = System.currentTimeMillis()
      println(s"Executed in ${(time1 - time0) / 1000.0} seconds")
      graph.edges.collect.foreach(println)


    }

}
