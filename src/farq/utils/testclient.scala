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

object TestClient
{
  
  Configgy.configure("farq.cfg")
  
  def sendSet() =
  {

    var data = new Array[Byte](10)
    
    data(0) = 1
    data(1) = 5
    data(2) = 6
    data(3) = 8
    data(4) = 8
    data(5) = 9
    
    var s = new Socket("127.0.0.1", 9999)
    
    var out = s.getOutputStream
    
    var dos = new DataOutputStream( out )
    
    var request = "SETMYKEY|".getBytes
    
    dos.write( request )
   
    dos.writeInt( data.length )
    
    dos.write( data, 0, data.length )
    

    s.close()    
  }

  def sendGet() =
  {


    
    var s = new Socket("127.0.0.1", 9999)
    
    var out = s.getOutputStream
    
    var dos = new DataOutputStream( out )
    
    var request = "GET".getBytes
    
    dos.write( request )
    Thread.sleep(100)
    
    var inp = s.getInputStream
    
    var dis = new DataInputStream( inp )
    
    var data = new Array[Byte](10)
    
    dis.read( data )
    
    println("data is " + data.toString() )
    
    s.close()    
  } 
  
  def main(args: Array[String]) =
  {

    sendSet()
    sendGet()
  }
}


