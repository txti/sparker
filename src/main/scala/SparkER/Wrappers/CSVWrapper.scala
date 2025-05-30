package SparkER.Wrappers

import SparkER.DataStructures.{MatchingEntities, Profile}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Luca on 11/12/2016.
  */
object CSVWrapper extends WrapperTrait {

  /**
    * Load the profiles from a CSV file.
    **/
  override def loadProfiles(filePath: String, startIDFrom: Long, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfiles2(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }

  /**
    * Load the profiles from a CSV file.
    *
    * @param filePath           the path of the dataset to load
    * @param startIDFrom        the ID which the autogenerated ID for profiles will start, the default value is 0
    * @param separator          specify the fields separator, the default value is ','
    * @param header             specify if the first CSV line is the header. The default value is false.
    * @param explodeInnerFields specify if the inner fields have to be splitted.
    *                           E.g. if a field contains a list of names separated by comma and you want to split it in single attributes.
    *                           The default value is false.
    * @param innerSeparator     if the parameter explodeInnerFields is true, specify the values separator.
    **/
  def loadProfiles2(filePath: String, startIDFrom: Long = 0, separator: String = ",", header: Boolean = false,
                   explodeInnerFields: Boolean = false, innerSeparator: String = ",", realIDField: String = "", sourceId: Int = 0): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns

    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator)).zipWithIndex().map {
      profile =>
        val profileID = profile._2 + startIDFrom
        val attributes = profile._1
        val realID = {
          if (realIDField.isEmpty) {
            ""
          }
          else {
            attributes.filter(_.key.toLowerCase() == realIDField.toLowerCase()).map(_.value).mkString("").trim
          }
        }
        Profile(profileID, attributes.filter(kv => kv.key.toLowerCase() != realIDField.toLowerCase()), realID, sourceId)
    }
  }

  override def loadGroundtruth(filePath: String): RDD[MatchingEntities] = {
    loadGroundtruth(filePath, ",", false)
  }

  /**
    * Load the groundtruth
    *
    * @param filePath  the path of the groundtruth
    * @param separator sepcify the fields separator, default ','
    * @param header    specify if the first row is the header. The default value is false
    **/
  def loadGroundtruth(filePath: String, separator: String = ",", header: Boolean = false): RDD[MatchingEntities] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).csv(filePath)
    df.rdd map {
      row =>
        MatchingEntities(row.get(0).toString, row.get(1).toString)
    }
  }
}
