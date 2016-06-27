import scala.io.Source
import scala.collection.mutable.HashMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * Created by Ilias Sarantopoulos on 4/27/16.
  * Degree centrality is calculated for each vertex as follows:
  * d(u) = u.degree / Total_number_of_vertices_in_graph -1
  */
object DegreeCentrality {

  def main(args: Array[String]) {
    val time0 = System.currentTimeMillis()
    //System.setProperty("hadoop.home.dir", "/home/lias/IdeaProjects/centrality/")
    //hdfs://sparkmaster:7077/user/ilias/twitter_edges.txt
    val conf = new SparkConf().setAppName("Degree")
    val sc = new SparkContext(conf)
    //val distFile = sc.textFile(edgesFile())
    var graph = GraphLoader.edgeListFile(sc, "hdfs://sparkmaster:9000/user/ilias/followers-new.txt")
    //var graph = GraphLoader.edgeListFile(sc, "twitter_edges.txt")
   // var graph = GraphLoader.edgeListFile(sc, "hdfs://sparkmaster:9000/user/ilias/twitter_edges.txt")
    /*var degrees:Map[Long,Int] = Map()
    for (triplet <- graph.triplets.collect) {
        println(s"${triplet.srcId} follows ${triplet.dstId}")}*/
    //var degrees: HashMap[Long, Int] = HashMap()
    //normalization factor is 1/node_count-1
    val normalizationFactor:Float = 1f/(graph.vertices.count()-1)
    val degrees: VertexRDD[Int] = graph.degrees.persist()
    println(s"The graph has a total of $degrees.count() vertices")
    //sort vertices on descending degree value
    val normalized = degrees.map((s => (s._1, s._2*normalizationFactor)))
    /*val users = sc.textFile("users.txt").map { line =>  val
    fields = line
      .split(",")
      (fields(0).toLong, fields(2))
    }*/
    /*val ranksByUsername = users.join(normalized).map {
      case (id, (username, score)) => (username, score)
    }*/
    //val sorted = ranksByUsername.sortBy(- _._2)
    val sorted = normalized.sortBy(-_._2)
    //print time elapsed
    val time1 = System.currentTimeMillis()
    println(s"Executed in ${(time1-time0)/1000.0} seconds")
    //print top 10 vertices
    for ((vertexId, degree) <- sorted.take(10)){
      println(s"User with id: ${vertexId} has a degree centrality of ${degree}")
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
