/*
 * Copyright (c) 2009, Ken Faulkner
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.

    * Neither the name of Ken Faulkner nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


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
      
      log.debug("entry data is " + entry.data.toString() )
      
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
      val os = socket.getOutputStream
      val dos = new DataOutputStream( os )
      
      if ( response._1.code == StatusCodes.GET_SUCCESS )
      {
        var entry = response._2
        
        // send 4 bytes.
        dos.writeInt( entry.data.length )
        
        // send data.
        dos.write( entry.data )
        
      }
      else
      {
        log.debug("sending response 0")
        // send 4 bytes. with 0, indicating kaput
        dos.writeInt( 0 )
        
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
      val is = socket.getInputStream
      val dis = new DataInputStream( is )

      // all we're after is the id.
      var id = dis.readInt()
            
      // bad form with !? dangerous?
      var resp = farq !? ( queueTimeout, (FARQCommands.delCommand, id ) )
      //farq ! (FARQCommands.setCommand, entry )
      
      log.debug("del response is " + resp.get.toString() )
            
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
