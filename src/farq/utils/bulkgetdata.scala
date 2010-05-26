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

package FARQ.utils

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import java.net._
import java.io._

object BulkGetData
{
  
  Configgy.configure("farq.cfg")

  var serverPort = Integer.parseInt( Configgy.config.getString("port", "9999" ) )
  var serverIP = Configgy.config.getString("server_ip", "127.0.0.1" ) 
  def sendGet() =
  {


    var quit = false
    
    while (!quit )
    {
      
      var s = new Socket(serverIP, serverPort)
      
      var out = s.getOutputStream
      
      var dos = new DataOutputStream( out )
      
      var request = "GET".getBytes
      
      dos.write( request )
      
      var inp = s.getInputStream
      
      var dis = new DataInputStream( inp )
      
      var l = dis.readInt()
      println("length is " + l.toString() )
      
      var data = new Array[Byte](10)
      
      dis.read( data )
      
      println("data is " + data.toString() )
      
      s.close()    
      if ( data(0) == 0 )
        quit = true
      
    }
    
  } 
  
  def main(args: Array[String]) =
  {

    sendGet()
  }
}


