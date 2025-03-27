package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.EntityClusterUtils.{
  addUnclusteredProfiles,
  connectedComponents
}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, Map}

object CenterClustering extends EntityClusteringTrait {
    override def getClusters(
        profiles: RDD[Profile],
        edges: RDD[WeightedEdge],
        maxProfileID: Long,
        edgesThreshold: Double,
        separatorID: Long
    ): RDD[(Long, Set[Long])] = {

        val filteredEdges = edges.filter(_.weight > edgesThreshold)

        val stats = filteredEdges
            .flatMap(x =>
                (x.firstProfileID, (x.weight, 1)) :: (
                    x.secondProfileID,
                    (x.weight, 1)
                ) :: Nil
            )
            .groupByKey()
            .map { x =>
                val edgesWeight = x._2.map(_._1).sum
                val edgesAttached = x._2.map(_._2).sum
                (x._1, edgesWeight / edgesAttached)
            }

        val statsB = SparkContext
            .getOrCreate()
            .broadcast(
                stats.collectAsMap()
            ) // <-- Warning: Expensive and costly action

        /** Generates the connected components */
        val cc = connectedComponents(filteredEdges)

        /** Then, in parallel, for each connected components computes the clusters
         */
        val res = cc.mapPartitions { partition =>
            /** Used to check if a profile is a center */
            val isCenter: Map[Long, Boolean] = Map()
            (0L to maxProfileID + 1).foreach { id =>
                isCenter(id) = false
            }

            /* Used to check if a profile was already added to a cluster */
            val isNonCenter: Map[Long, Boolean] = Map()
                (0L to maxProfileID + 1).foreach { id =>
                isNonCenter(id) = false
            }

            /* Generated clusters */
            val clusters = Map[Long, Set[Long]]()

            /** Foreach connected component */
            partition.foreach { cluster =>
                /* Sorts the elements in the cluster descending by their similarity score */
                val sorted = cluster.toList.sortBy(x => (-x._3, x._1))

                /* Foreach element in the format (u, v) */
                sorted.foreach { case (u, v, sim) =>
                    val uIsCenter = isCenter(u)
                    val vIsCenter = isCenter(v)
                    val uIsNonCenter = isNonCenter(u)
                    val vIsNonCenter = isNonCenter(v)

                    if (!(uIsCenter || vIsCenter || uIsNonCenter || vIsNonCenter)) {
                        val w1 = statsB.value.getOrElse(u, 0.0)
                        val w2 = statsB.value.getOrElse(v, 0.0)

                        if (w1 > w2) {
                            clusters.put(u, Set(u, v))
                            isCenter.update(u, true)
                            isNonCenter.update(v, true)
                        } else {
                            clusters.put(v, Set(u, v))
                            isCenter.update(v, true)
                            isNonCenter.update(u, true)
                        }
                    } else if ((uIsCenter && vIsCenter) || (uIsNonCenter && vIsNonCenter)) {
                        // TODO:  Nothing? Bug?
                    } else if (uIsCenter && !vIsNonCenter) {
                        clusters.put(u, clusters(u) + v)
                        isNonCenter.update(v, true)
                    } else if (vIsCenter && !uIsNonCenter) {
                        clusters.put(v, clusters(v) + u)
                        isNonCenter.update(u, true)
                    }
                }
            }

            /*
            *
            * /* Used to check if a profile was already added to a cluster */
            val visited = Array.fill[Boolean](maxProfileID + 1) {
                false
            }
            /* Generated clusters */
            val clusters = scala.collection.mutable.Map[Long, Set[Long]]()

            /** Foreach connected component */
            partition.foreach { cluster =>
                /* Sorts the elements in the cluster descending by their similarity score */
                val sorted = cluster.toList.sortBy(x => (-x._3, x._1))

                /* Foreach element in the format (u, v) */
                sorted.foreach { case (u, v, sim) =>

                val uIsCenter = clusters.contains(u)
                val vIsCenter = clusters.contains(v)
                val uIsNonCenter = visited(u)
                val vIsNonCenter = visited(v)

                if (!(uIsCenter || vIsCenter || uIsNonCenter || vIsNonCenter)) {
                    val w1 = statsB.value.getOrElse(u, 0.0)
                    val w2 = statsB.value.getOrElse(v, 0.0)

                    if (w1 > w2) {
                    clusters.put(u, Set(u, v))
                    visited.update(u, true)
                    visited.update(v, true)
                    }
                    else {
                    clusters.put(v, Set(u, v))
                    visited.update(u, true)
                    visited.update(v, true)
                    }
                }
                else if ((uIsCenter && vIsCenter) || (uIsNonCenter && vIsNonCenter)) {}
                else if (uIsCenter && !vIsNonCenter) {
                    clusters.put(u, clusters(u) + v)
                    visited.update(v, true)
                }
                else if (vIsCenter && !uIsNonCenter) {
                    clusters.put(v, clusters(v) + u)
                    visited.update(u, true)
                }
                }
            }
            * */

            clusters.toIterator
        }

        addUnclusteredProfiles(profiles, res)
    }
}
