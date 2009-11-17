package FARQ.utils

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import java.net._
import java.io._

object GetData
{
  
  Configgy.configure("farq.cfg")


  def sendGet() =
  {


    
    var s = new Socket("127.0.0.1", 9999)
    
    var out = s.getOutputStream
    
    var dos = new DataOutputStream( out )
    
    var request = "GET".getBytes
    
    dos.write( request )
    
    var inp = s.getInputStream
    
    var dis = new DataInputStream( inp )
    
    var data = new Array[Byte](10)
    
    dis.read( data )
    
    println("data is " + data.toString() )
    
    s.close()    
  } 
  
  def main(args: Array[String]) =
  {

    sendGet()
  }
}


