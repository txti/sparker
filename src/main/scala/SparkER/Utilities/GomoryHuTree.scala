package SparkER.Utilities

import org.jgrapht.Graphs
import org.jgrapht.UndirectedGraph
import org.jgrapht.alg.MinSourceSinkCut
import org.jgrapht.graph.{DefaultDirectedWeightedGraph, DefaultEdge, DefaultWeightedEdge, SimpleGraph, SimpleWeightedGraph}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 *
 * @author manos
 */
class GomoryHuTree[V, E](protected val graph: SimpleWeightedGraph[V, E]) {

  private def makeDirectedCopy(graph: UndirectedGraph[V, E]): DefaultDirectedWeightedGraph[V, DefaultWeightedEdge] = {
    val copy = new DefaultDirectedWeightedGraph[V, DefaultWeightedEdge](classOf[DefaultWeightedEdge])

    Graphs.addAllVertices(copy, graph.vertexSet())
    for (e <- graph.edgeSet().asScala) {
      val v1 = graph.getEdgeSource(e)
      val v2 = graph.getEdgeTarget(e)
      Graphs.addEdge(copy, v1, v2, graph.getEdgeWeight(e))
      Graphs.addEdge(copy, v2, v1, graph.getEdgeWeight(e))
    }

    copy
  }

  def MinCutTree(): SimpleGraph[Long, DefaultEdge] = {
    val directedGraph = makeDirectedCopy(graph)

    val predecessors = mutable.Map[V, V]()
    val vertexSet = directedGraph.vertexSet()
    val it = vertexSet.iterator
    val start = it.next()

    predecessors.put(start, start)

    while (it.hasNext) {
      val vertex = it.next()
      predecessors.put(vertex, start)
    }

    val returnGraphClone = new DefaultDirectedWeightedGraph[V, DefaultWeightedEdge](classOf[DefaultWeightedEdge])
    val returnGraph = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
    val minSourceSinkCut = new MinSourceSinkCut[V, DefaultWeightedEdge](directedGraph)

    val itVertices = directedGraph.vertexSet().iterator
    itVertices.next() // Skip the first vertex (start)
    while (itVertices.hasNext) {
      val vertex = itVertices.next()
      val predecessor = predecessors(vertex)
      minSourceSinkCut.computeMinCut(vertex, predecessor)

      returnGraphClone.addVertex(vertex)
      returnGraphClone.addVertex(predecessor)

      returnGraph.addVertex(vertex.toString.toLong)
      returnGraph.addVertex(predecessor.toString.toLong)

      val sourcePartition = minSourceSinkCut.getSourcePartition
      val flowValue = minSourceSinkCut.getCutWeight
      val e = returnGraphClone.addEdge(vertex, predecessor).asInstanceOf[DefaultWeightedEdge]
      returnGraph.addEdge(vertex.toString.toLong, predecessor.toString.toLong)
      returnGraphClone.setEdgeWeight(e, flowValue)

      for (sourceVertex <- graph.vertexSet().asScala) {
        if (predecessors(sourceVertex) == predecessor && sourcePartition.contains(sourceVertex)) {
          predecessors.put(sourceVertex, vertex)
        }
      }
    }

    returnGraph
  }
}
