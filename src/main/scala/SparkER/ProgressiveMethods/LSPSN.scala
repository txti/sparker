package SparkER.ProgressiveMethods

import SparkER.BlockBuildingMethods.BlockingUtils
import SparkER.DataStructures.Profile
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

class LSPSN(profiles: RDD[Profile], maxWindowSize: Int, separatorID: Long = -1) extends Serializable {

  private var currentWindowSize: Int = 1
  private var positionIndex: Broadcast[scala.collection.Map[Long, Iterable[Long]]] = null
  private var neighborList: Broadcast[scala.collection.Map[Long, Long]] = null
  private var comparisons: List[(Long, Long, Double)] = List.empty

  /** Initiliaze the datastructures */
  def initialize(): Unit = {
    val sc = profiles.context
    currentWindowSize = 1

    /** For every token of every profile, emits the pair (token, profileID) */
    val tokens = profiles.flatMap { p =>
      p.attributes.flatMap { attr =>
        val set = attr.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).toSet
        set.map(t => (t, p.id))
      }
    }

    /** Sorts the token in alphabetical order, and replace them with a unique id which represents the position
      * The result is an rdd of (position, profileID)
      * */
    val sorted = tokens.sortBy(_._1).zipWithIndex().map(x => (x._2, x._1._2))

    /** Position index: given a profileID returns all the positions in which appears */
    positionIndex = sc.broadcast(sorted.map(_.swap).groupByKey().collectAsMap())

    /** neighborList: given a position returns the id of the profile contained in that position */
    neighborList = sc.broadcast(sorted.collectAsMap())
  }

  /** Computes the comparisons to emit */
  def computeComparisons(): Unit = {
    /** For every profile in the RDD */
    val results = profiles.mapPartitions { part =>
      //Neighbors list
      val neighbors: Map[Long, Long] = Map()

      //Number of neighbors
      var neighborsNum = 0L
      //Common Block Schema
      val cbs: Map[Long, Long] = Map()
      //Results
      var res: List[(Long, Long, Double)] = Nil

      /** For every partition of the RDD */
      part.foreach { profile =>

        /** Reads the positions in which the profile appears */
        val positions = positionIndex.value(profile.id)
        positions.foreach { pos =>

          /** Reads the neighbors at the windowSize distance, after and before */
          for (i <- -1 to 1 by 2) {
            val w = (currentWindowSize * i)

            /** Checks that w is inside the array */
            if ((w > 0 && (pos + w) < neighborList.value.size) || (w < 0 && (pos + w) > 0)) {
              /** Get the id of the neighbor at the given position */
              val pi = neighborList.value(pos + w)

              /** Checks to avoid double emissions */
              if (pi < profile.id) {
                //Checks that pi comes from the first dataset, while profile from the second one
                if ((separatorID < 0) || (pi <= separatorID && profile.id > separatorID)) {

                  /** If it is the first time, add it to the neighbors list */
                  if (cbs(pi) == 0) {
                    neighbors.update(neighborsNum, pi)
                    neighborsNum += 1
                  }

                  /** Increase the CBS */
                  cbs.update(pi, cbs(pi) + 1)
                }
              }
            }
          }
        }

        //Computes the weights and add the pair to the list
        (0L until neighborsNum) foreach { i =>
          val currentWindowSize = maxWindowSize / (i + 1)
          val pos = positionIndex.value(i)
          val nId = neighbors(i)
          val weight = cbs(nId) / (positions.size + positionIndex.value(nId).size - cbs(nId)).toDouble
          res = (neighbors(i), profile.id, weight) :: res
        }

        //Reset for next iteration
        (0L until neighborsNum) foreach { i =>
          cbs.update(neighbors(i), 0)
        }

        neighborsNum = 0L
      }

      res.toIterator
    }

    comparisons = results.sortBy(-_._3).collect().toList
  }

  /**
    * Returns the best next comparison
    **/
  def getNextComparison: (Long, Long, Double) = {
    var cmp: (Long, Long, Double) = (-1, -1, 0.0)
    if (comparisons.nonEmpty) {
      cmp = comparisons.head
      comparisons = comparisons.tail
    }
    else if (currentWindowSize <= maxWindowSize) {
      computeComparisons()
      currentWindowSize += 1
      if (comparisons.nonEmpty) {
        cmp = comparisons.head
        comparisons = comparisons.tail
      }
    }
    cmp
  }
}
