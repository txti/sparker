package SparkER.SimJoins.DataStructure

case class DocIndex(pos: Int, docLen: Int)

//listDocId_tokenPos_numTokens
case class TokenDocumentInfo(docId: Long, pos: Int, docLen: Int)
