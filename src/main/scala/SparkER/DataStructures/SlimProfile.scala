package SparkER.DataStructures

case class SlimProfile(id: Long, attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList(), originalID: String = "", sourceId: Int = 0)
  extends SlimProfileTrait with Serializable {

}
  object  SlimProfile{

  def apply(profile : Profile): SlimProfile = {
    SlimProfile(profile.id, profile.attributes, profile.originalID, profile.sourceId)
  }
}
