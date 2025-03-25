package SparkER.EntityClustering

import SparkER.DataStructures.{Profile, WeightedEdge}
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.apache.spark.rdd.RDD

object ConnectedComponentsClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Long, edgesThreshold: Double = 0, separatorID: Long = -1): RDD[(Long, Set[Long])] = {
    val cc = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val a = cc.map(x => x.flatMap(y => y._1 :: y._2 :: Nil)).zipWithIndex().map(x => (x._2, x._1.toSet))
    addUnclusteredProfiles(profiles, a)
  }
}
