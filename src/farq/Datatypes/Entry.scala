package FARQ.Datatypes

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import net.lag.configgy.Configgy
import net.lag.logging.Logger

@serializable
case class Entry( k: String)
{

  var key = k
  var timeStamp = 0
  var data:Array[Byte] = null
  
  // used for get/delete
  // boolean could probably go... but will leave it.
  var isInvisible = false
  var invisibleTimer = -1
  
  
}
