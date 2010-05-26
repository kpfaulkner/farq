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
      react
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
  val backlog =  Integer.parseInt( Configgy.config.getString("backlog", "100" ) )
  
  def run() =
  {
  	// accept connetion and pass to the dispatcher actor.
    val socket = new ServerSocket( port, backlog )
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
