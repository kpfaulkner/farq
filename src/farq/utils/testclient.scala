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
    
    s.close()    
  } 
  
  def main(args: Array[String]) =
  {

    sendSet()
    sendGet()
  }
}


