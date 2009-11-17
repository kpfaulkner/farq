/* Copyright Ken Faulkner 2009 */

package FARQ

import scala.actors.Actor
import scala.actors.Actor._
import java.net._
import java.io._
import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.collection.mutable.ListBuffer

case class Connection(socket: Socket, id: Int)


class Dispatcher() extends Actor
{

  Configgy.configure("farq.cfg")
  val log = Logger.get

  val handlers = new ListBuffer[FARQHandler]()
  var currentHandler = 0
  var maxHandlers =  Integer.parseInt( Configgy.config.getString("max_handlers", "10" ) )
  

  var farq = new FARQueue()
  
  farq.start()
  
  def populateHandler() =
  {
    for ( i <- 0 until maxHandlers )
    {
      val w = new FARQHandler( farq )
      w.start()
      handlers += w
    }
    
  }
  
  // sure I should just use an iterator.
  def getHandler( ): FARQHandler =
  {
    log.debug("getHandler " + currentHandler.toString() )
    var w = handlers( currentHandler )
    
    currentHandler += 1
    if (currentHandler >= maxHandlers )
      currentHandler = 0
    
    return w
  }
  
    
  populateHandler()
  
  def act()
  {
    loop
    {
      receive
      {
        case conn: Connection =>
          val handler = getHandler()
          handler ! conn
      }
    }
    
  }
}


class Server( port:Int )
{
  val name: String = "Main"
  val log = Logger.get
  
  val bufferSize =  Integer.parseInt( Configgy.config.getString("buffer_size", "200000" ) )
  
  def run() =
  {
  	// accept connetion and pass to the dispatcher actor.
    val socket = new ServerSocket( port )
    var buf = socket.getReceiveBufferSize()
    socket.setReceiveBufferSize( bufferSize )
    buf = socket.getReceiveBufferSize()
    
    val dispatcher = new Dispatcher()
    var i = 0

    dispatcher.start()

    while (true)
    {
      val clientConn = socket.accept()
      i += 1
      dispatcher ! Connection(clientConn, i)
    }
  }
}


object farq
{
  Configgy.configure("farq.cfg")
  
  def main(args: Array[String]) =
  {
    val port = Integer.parseInt( Configgy.config.getString("port", "9999" ) )
    new Server( port ).run()
  }
}
