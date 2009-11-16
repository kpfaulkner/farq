package FARQ

import scala.actors.Actor
import scala.actors.Actor._

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.collection.mutable.Queue

import FARQ.Datatypes._

class FARQueue extends Actor
{

  val log = Logger.get

  // main queue.
  var queue = new Queue[ Entry ]()
  
  // put here if we're trying to retrieve it.
  // will be invisible for a while, but then returned to visible 
  // if not deleted.
  var invisibleQueue = new Queue[ Entry] ()
  
  def act()
  {
    loop
    {
      react
      {
        case ( FARQCommands.setCommand, entry:Entry) =>
        { 
          handleSet( entry )
  
          // bad form?
          sender ! new Status( StatusCodes.SET_SUCCESS )
          
        }
        
        case FARQCommands.getCommand =>
        {
          // get the latest.
          
          var e = handleGet()
          var statusCode = StatusCodes.GET_SUCCESS
          
          if ( e == null )
          {
            statusCode = StatusCodes.GET_EMPTY
          }
          
          // potentially returning a numm entry... sucky...
          sender ! ( new Status(  statusCode ), e)
        }
        
        case ( FARQCommands.getCommand, key:String ) =>
        {
          // delete existing one.
        }

      }
    }
  }
  
  def handleSet( entry:Entry ) =
  {
    log.debug("FARQueue::handleSet start")
    
    log.debug("entry is " + entry.toString() )
    log.debug("entry key is " + entry.key )
    
    queue += entry
    
  }

  // shift from real Q to invisible Q.
  def handleGet( ): Entry =
  {
    log.debug("FARQueue::handleGet start")
    
    var entry:Entry = null
    
    try
    {
      entry = queue.dequeue
      
      entry.isInvisible = true
      
      invisibleQueue += entry
    
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQueue::handleGet exception " + ex.toString() )
    } 
    
    return entry
  }
    
}
