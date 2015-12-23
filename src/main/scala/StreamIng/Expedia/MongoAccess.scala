package StreamIng.Expedia

//import com.mongodb.casbah.Imports._

object MongoAccess {
  def getMongoCollection(name: String) = {
    val address = "10.2.13.12"
    val port = 27017
    val database = "LpasPricingAnalysis"
    //val mongoClient = MongoClient(address, port)
    //val db = mongoClient(database)
    //db(name)
  }
}


