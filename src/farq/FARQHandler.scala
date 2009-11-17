/* Copyright Ken Faulkner 2009 */

package FARQ

import scala.actors.Actor
import scala.actors.Actor._
import java.net._
import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.collection.mutable.ArrayBuffer
import FARQ.Datatypes._


object FARQCommands
{
  val getCommand = "GET"
  val setCommand = "SET"
  val delCommand = "DEL"
  
}


class FARQHandler(fq: FARQueue) extends Actor
{



  var farq = fq
  val log = Logger.get
    
  val queueTimeout = Integer.parseInt( Configgy.config.getString("queue_timeout_ms", "1000" ) )
  
  def act()
  {
    loop
    {
      receive
      {
        case Connection(socket, id) =>
         
          handleConnection(socket)
          socket.close()

      }
    }
  }

  def getKey( dis: DataInputStream ): String =
  {
      // get key. Basically read until we get a |
      var key = ""
      var gotKey = false
      
      while ( !gotKey )
      {
        
        // read bytes since readChar is unicode.
        var byte = dis.readByte()
        var ch = byte.toChar
        if ( ch == '|' )
        {
          gotKey = true
        }
        else
        {
          key += ch
        }      
      }
      
      return key
  }
  
  
  // set something in the far-q. :)
  def handleSet( socket: Socket ) = 
  {
    log.info("FARQHandler::handleSet start")
    
    try
    {
      
      
      val is = socket.getInputStream
      val dis = new DataInputStream( is )

      var key = getKey( dis )
      
      // get length of data.
      var length = dis.readInt()
      
      var buffer = new Array[Byte](1024)
      var count = 0
      var data = new ArrayBuffer[Byte]()
      
      while ( count < length )
      {
        var res = dis.read( buffer )
        count += res
      
        // append the data read.
        data ++= ( buffer, 0, res )
        
      }
  
      
      var entry = new Entry( key )
      entry.data = data.toArray
      
      // bad form with !? dangerous?
      var resp = farq !? ( queueTimeout, (FARQCommands.setCommand, entry ) )
      //farq ! (FARQCommands.setCommand, entry )
      
      log.debug("set response is " + resp.get.toString() )
      
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQHandler::handleSet exception " + ex.toString() )
    } 
  }
  
  
  def handleGet( socket: Socket) = 
  {
    log.info("FARQHandler::handleGet start")
    //val is = socket.getInputStream
    //val dis = new DataInputStream( is )
    
    try
    {

      var resp = farq !? ( queueTimeout , FARQCommands.getCommand   )
      
      var response = resp.get.asInstanceOf[ (Status, Entry)]
      
      log.debug("status is " + response._1.toString() )
      
      if ( response._1.code == StatusCodes.GET_SUCCESS )
      {
        var entry = response._2
        
        val os = socket.getOutputStream
        val dos = new DataOutputStream( os )
        
        log.debug("writing " + entry.data.toString() )
        dos.write( entry.data )
        
      }

    }
    catch
    {
      case ex: Exception =>
        log.error("FARQHandler::handleGet exception " + ex.toString() )
    } 
  }

  def handleDel(  socket: Socket ) = 
  {
    log.info("FARQHandler::handleDel start")
    //val is = socket.getInputStream
    //val dis = new DataInputStream( is )
    try
    {
      
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQHandler::handleDel exception " + ex.toString() )
    }    
  }  
  
  def handleConnection(socket: Socket): String =
  {

    log.info("FARQHandler::handleConnection start")
    
    try
    {
      
      // read url.
      val is = socket.getInputStream
      
      val dis = new DataInputStream( is )
      
      // read first 3 bytes to determine what the operation is.
      var op = new Array[Byte](3)
      
      dis.read( op, 0, 3)
      
      op match
      {
        
        case Array(83,69,84) =>
        {
          handleSet( socket )
        }
        case Array(71,69,84) =>
        {
          handleGet( socket )
        }
        case Array(68,69,76) =>
        {
          handleDel( socket )
        }
        
        case _ =>
        {
          log.debug("unknown")
        }
      }
       
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQHandler::handleConnection exception " + ex.toString() )
    } 
      
    return ""
  }


}
