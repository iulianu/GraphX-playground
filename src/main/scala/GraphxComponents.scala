import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
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

  val emptySPMap: SPMap = Map.empty[VertexId, Int]
  
  case class Adjacency(source: EdgeSource, validAt: Long)

  case class EdgeTag(adjacencies: Seq[Adjacency], distanceMap: SPMap)

  case class VertexTag(id: String, devType: DeviceType)

  /* Syntactic sugar */
  def singleTag(source: EdgeSource, validAt: Long) =
    EdgeTag(Seq(Adjacency(source, validAt)), emptySPMap)
}

import DeviceGraphTypes._

object GraphxComponents extends App {

  val conf = new SparkConf().setAppName("GraphX device graph demo")
    .setMaster("local[2]")

  val sc = new SparkContext(conf)

  val deviceInfo: RDD[(VertexId, VertexTag)] =
    sc.parallelize(Array(
      (1L, VertexTag("A", Browser("Mozilla/6.1 Safari"))),
      (7L, VertexTag("B", Browser("Mozilla/6.0"))),
      (11L, VertexTag("C", Android)),
      (12L, VertexTag("C2", Browser("Mozilla/5.0 Chrome/48"))),
      (13L, VertexTag("D", IOS)),
      (18L, VertexTag("E", Android)),
      (22L, VertexTag("F", IOS)),
      (34L, VertexTag("G", Browser("Mozilla/5.0"))),
      (35L, VertexTag("G2", Browser("Mozilla/5.0"))),
      (36L, VertexTag("G3", Browser("Mozilla/5.0"))),
      (12L, VertexTag("H", Android)),
      (21L, VertexTag("J", IOS)),
      (34L, VertexTag("K", Browser("IE 11"))),
      (35L, VertexTag("L", IOS)),
      (41L, VertexTag("M", Browser("Mozilla/5.0 Firefox/40.0.3")))
    ))

  val LastImportTimestamp = 100L

  val deviceAdjacency: RDD[Edge[EdgeTag]] =
    sc.parallelize(Array(
      Edge(11L, 22L, singleTag(KO2O, LastImportTimestamp)),
      Edge(22L, 36L, singleTag(Tapad, LastImportTimestamp)),
      Edge(12L, 21L, singleTag(KO2O, LastImportTimestamp)),
      Edge(21L, 34L, singleTag(Tapad, LastImportTimestamp)),
      Edge(21L, 35L, singleTag(Tapad, LastImportTimestamp)),
      Edge(21L, 41L, singleTag(JDPA, LastImportTimestamp)),
      Edge(13L, 41L, singleTag(KO2O, LastImportTimestamp)),
      Edge(18L, 1L, singleTag(KO2O, LastImportTimestamp)),
      Edge(1L, 7L, singleTag(JDPA, LastImportTimestamp))
  ))

  val graph = Graph(deviceInfo, deviceAdjacency)
 // BEGIN connected components
  /*
  val ccGraph = graph.connectedComponents()

  println(ccGraph.vertices.collect().toList)
  */
  // END connected components

  val filteredIds : Seq[VertexId] = Seq(11, 12, 13, 18)


  // BEGIN weighted Shortest paths
  //
  // See the unweighted version in the Spark GraphX library:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala
  //
  // See the Pregel API docs at
  // http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api

  val OneWeek: Long = 7 * 24 * 60 * 60 * 1000L

  val weights = graph.mapTriplets { edge =>
    val adjacencies = edge.attr.adjacencies.
      sortBy(- _.validAt) // Newest adjacencies are sorted first.

    // Assign weight based on adjacencies
    // Rules can be as complex as we want.
    // The rules below are made up just to prove the point.

    if( adjacencies.isEmpty )
      Int.MaxValue // Absolute lowest weight if no adjacencies.
    else if( adjacencies.exists(_.source == KO2O) )
      // Highest weight if at least one adjacency from KO2O has ever existed.
      1
    else if( System.currentTimeMillis - adjacencies.head.validAt > OneWeek )
      // Lower weight if the most recent adjacency is too old
      4
    else if( adjacencies.exists(_.source == JDPA) )
      2
    else if( adjacencies.forall(_.source == Tapad) )
      // Not just one but all adjacencies come from Tapad
      4
    else
      // Even lower weight in all other cases
      16
  }

  def incrementMap(spmap: SPMap, distance: Int): SPMap
    = spmap.map { case (v, d) => v -> (d + distance) }

  def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  val spGraph = weights.mapVertices { (vid, attr) =>
    // when the filtered ids are joined to the weights we can
    // check the attr instead of checking this sequence
    if (filteredIds.contains(vid))
      Map(vid -> 0)
    else
      emptySPMap
  }

  val initialMessage = emptySPMap

  def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
    addMaps(attr, msg)
  }

  def sendMessage(edge: EdgeTriplet[SPMap, Int]): Iterator[(VertexId, SPMap)] = {
    val dstAttr = incrementMap(edge.dstAttr, edge.attr)
    val srcAttr = incrementMap(edge.srcAttr, edge.attr)

    if (edge.srcAttr != addMaps(dstAttr, edge.srcAttr)) Iterator((edge.srcId, dstAttr))
    else if (edge.dstAttr != addMaps(srcAttr, edge.dstAttr)) Iterator((edge.dstId, srcAttr))
    else Iterator.empty
  }

  val distances = Pregel(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Either)(vertexProgram, sendMessage, addMaps)

  println(distances.vertices.take(20).mkString(" "))
}
