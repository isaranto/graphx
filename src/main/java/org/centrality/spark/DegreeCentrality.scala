
import scala.io.Source
import scala.collection.mutable.HashMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * Created by Ilias Sarantopoulos on 4/27/16.
  */
object DegreeCentrality {

  def calcDegree() : Map[Long,Int] = {
      var degrees:Map[Long,Int] = Map()
      //degrees += (15 -> 1)
      return degrees

   }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //val distFile = sc.textFile(edgesFile())
    //val graph = GraphLoader.edgeListFile(sc, "twitter_edges.txt")
    val graph = GraphLoader.edgeListFile(sc, "followers.txt")
    /*var degrees:Map[Long,Int] = Map()
    for (triplet <- graph.triplets.collect) {
        println(s"${triplet.srcId} follows ${triplet.dstId}")}*/
    //var degrees: HashMap[Long, Int] = HashMap()
    //normalization factor is 1/node_count-1
    val normalizationFactor:Float = 1f/(graph.vertices.count()-1)
    val degrees: VertexRDD[Int] = graph.degrees
    println(s"The graph has a total of $degrees.count() vertices")
    //sort vertices on descending degree value
    val normalized = degrees.map((s => (s._1, s._2*normalizationFactor)))
    val sorted = normalized.sortBy(- _._2)
    //print top 10 vertices
    for ((vertexId, degree) <- sorted.take(10)){
      println(s"Vertex with id ${vertexId} has a degree of ${degree}")
    }
    /*
    alternative way to calculate degree centrality
    val results = graph.degrees.join(graph.vertices).sortBy(_._2._1,
      ascending=false).take(10)
    for (triplet <- results){
      println(triplet._1,triplet._2._1)
    }*/

  }


}
