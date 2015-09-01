import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object DeviceGraphTypes {

  sealed trait EdgeSource
  case object KO2O  extends EdgeSource
  case object JDPA  extends EdgeSource
  case object Tapad extends EdgeSource

  sealed trait DeviceType
  case class Browser(userAgent: String)
                      extends DeviceType
  case object IOS     extends DeviceType
  case object Android extends DeviceType

  case class GraphAdjacencyTag(source: EdgeSource, validAt: Long)

  /* Syntactic sugar */
  def singleTag(source: EdgeSource, validAt: Long) =
    Seq(GraphAdjacencyTag(source, validAt))

  type EdgeTag = Seq[GraphAdjacencyTag]

  case class VertexTag(id: String, devType: DeviceType)
}

import DeviceGraphTypes._

object GraphxComponents extends App {

  val conf = new SparkConf().setAppName("GraphX device graph demo")
    .setMaster("local[2]")

  val sc = new SparkContext(conf)

  val deviceInfo: RDD[(VertexId, VertexTag)] =
    sc.parallelize(Array(
      (1L, VertexTag("A", Android)),
      (2L, VertexTag("B", IOS)),
      (3L, VertexTag("C", Browser("Mozilla/5.0"))),
      (4L, VertexTag("D", Android)),
      (5L, VertexTag("E", IOS))))

  val deviceAdjacency: RDD[Edge[EdgeTag]] =
    sc.parallelize(Array(
      Edge(1L, 2L, singleTag(Tapad, 100L)),
      Edge(1L, 3L, singleTag(Tapad, 100L)),
      Edge(2L, 3L, singleTag(Tapad, 100L)),
      Edge(2L, 4L, singleTag(Tapad, 100L)),
      Edge(4L, 5L, singleTag(Tapad, 100L))
  ))

  val graph = Graph(deviceInfo, deviceAdjacency)
  /*
  println(graph.vertices.count)
  println(graph.edges.count)
  println(graph.vertices.first)

  val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is related in strength value as " + triplet.attr + " of " + triplet.dstAttr._1)
  facts.collect.foreach(println(_))

  println(graph.collectNeighborIds(EdgeDirection.Out))

  println(graph.collectNeighbors(EdgeDirection.Out))
  println(graph.vertices.collect().toList)
*/

  val ccGraph = graph.connectedComponents()

  println(ccGraph.vertices.collect().toList)
}
