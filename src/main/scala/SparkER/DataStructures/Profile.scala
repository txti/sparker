package SparkER.DataStructures

/**
  * Represents a profile
  *
  * @author Giovanni Simonini
  * @since 2016/07/12
  */
case class Profile(id: Long,
	attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList(),
	originalID: String = "",
	sourceId: Int = 0) extends ProfileTrait with Serializable {

  /**
    * Add an attribute to the list of attributes
    *
    * @param a attribute to add
    **/
  def addAttribute(a: KeyValue): Unit = {
    attributes += a
  }

  // todo If we have no attributes (e.g. a single doc), we have a single element in the list
}
