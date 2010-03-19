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


package FARQ.client

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import java.net._
import java.io._
import scala.collection.mutable.ArrayBuffer

class FARQClient
{
  Configgy.configure("farq.cfg")
  val log = Logger.get
  
  var serverIP =  Configgy.config.getString("server_ip", "127.0.0.1" ) 
  var serverPort =  Integer.parseInt( Configgy.config.getString("server_port", "9999" ) ) 
    
  
  // assume the client has already created array of bytes.
  def push( queueName:String, data: Array[Byte] ) =
  {
        
    log.info("FARQClient::push start")
    
    log.debug("server " + serverIP )
    log.debug("port " + serverPort.toString())
    
    var s = new Socket( serverIP, serverPort)
    var out = s.getOutputStream
    var dos = new DataOutputStream( out )

    var request = "SET"+queueName+"|"
    var req = request.getBytes
    
    dos.write( req  )
   
    dos.writeInt( data.length )
    
    dos.write( data, 0, data.length )
    
    s.close()
  }

  
  def pop( queueName: String ): Array[Byte ] =
  {


    
    var s = new Socket( serverIP, serverPort)
    
    var out = s.getOutputStream    
    var dos = new DataOutputStream( out )
    
    var request = "GET".getBytes
    
    dos.write( request )
    
    var inp = s.getInputStream
    var dis = new DataInputStream( inp )
    
    var length = dis.readInt()
    var data = new ArrayBuffer[Byte]()
      
    // length of data to read.
    if ( length > 0 )
    {
      var buffer = new Array[Byte](1024)
      var count = 0
      
      while ( count < length )
      {
        var res = dis.read( buffer )
        count += res
      
        // append the data read.
        data ++= ( buffer, 0, res )    
      }
      
    }
    
    s.close()    
    
    return data.toArray
    
  } 
    
  
  
}



