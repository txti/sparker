package Experiments

import java.util.Calendar

import SparkER.DataStructures.WeightedEdge
import SparkER.EntityClustering.{ConnectedComponentsClustering, EntityClusterUtils}
import SparkER.SimJoins.Commons.CommonFunctions
import SparkER.SimJoins.SimJoins.EDJoin
import SparkER.Wrappers.CSVWrapper
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Tests the EDJoin implementation
  **/
object EDJoinExp {
  def main(args: Array[String]): Unit = {

    /* Dataset to test */
    val dataset = "restaurant"

    /* Base path where the dataset is located */
    val basePath = s"python/datasets/dirty/${dataset}"

    /* Profiles to join */
    val filePath = s"${basePath}/${dataset}.csv"

    /* Groundtruth */
    val gtPath = s"${basePath}/${dataset}_groundtruth.csv"

    /** Log file */
    val logPath = "log.txt"

    /** Spark configuration */
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logPath, false)
    log.addAppender(appender)

    /* Loads the profiles */
    val profiles = CSVWrapper.loadProfiles2(filePath, realIDField = "id", header = true)

    /** Extracts the attributes on which perform the join */
    val name = CommonFunctions.extractField(profiles, "name")
    name.cache()
    name.count()

    val addr = CommonFunctions.extractField(profiles, "addr")
    addr.cache()
    addr.count()

    /** Performs the join on the extracted attributes */
    val t1 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Join: matches_given_name")
    val matches_name = EDJoin.getMatches(name, 2, 2)
    log.info("[EDJoin] Join: matches_surname")
    val matches_addr = EDJoin.getMatches(addr, 2, 2)
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)

    /** Performs the intersection of the two result sets */
    log.info("[EDJoin] Join: intersection")
    val matches = matches_name.intersection(matches_addr)
    matches.cache()
    val nm = matches.count()
    val t3 = Calendar.getInstance().getTimeInMillis
    matches_name.unpersist()
    matches_addr.unpersist()
    log.info("[EDJoin] Number of matches " + nm)
    log.info("[EDJoin] Intersection time (s) " + (t3 - t2) / 1000.0)

    /** Perform the transitive closure of the results */
    val clusters = ConnectedComponentsClustering.getClusters(profiles, matches.map(x => WeightedEdge(x._1, x._2, 0)), 0)
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Number of clusters " + cn)
    log.info("[EDJoin] Clustering time (s) " + (t4 - t3) / 1000.0)

    log.info("[EDJoin] Total time (s) " + (t4 - t1) / 1000.0)

    /** Loads the groundtruth */
    val groundtruth = CSVWrapper.loadGroundtruth(gtPath)

    /** Converts the ids in the groundtruth to the autogenerated ones */
    val realIdIds = sc.broadcast(profiles.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())

    var newGT: Set[(Long, Long)] = null
    newGT = groundtruth.map { g =>
      val first = realIdIds.value.get(g.firstEntityID)
      val second = realIdIds.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        val f = first.get
        val s = second.get
        if (f < s) (f, s) else (s, f)
      }
      else {
        (-1: Long, -1: Long)
      }
    }.filter(_._1 >= 0).collect().toSet


    log.info("[EDJoin] Groundtruth size " + groundtruth.count())
    log.info("[EDJoin] New groundtruth size " + newGT.size)

    val gt = sc.broadcast(newGT)

    /** Computes precision and recall */
    val pcpq = EntityClusterUtils.calcPcPqCluster(clusters, gt)
    log.info("[EDJoin] PC " + pcpq._1)
    log.info("[EDJoin] PQ " + pcpq._2)

    val f1 = 2 * ((pcpq._1 * pcpq._2) / (pcpq._1 + pcpq._2))
    log.info("[EDJoin] F1 " + f1)
  }
}
