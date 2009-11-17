package FARQ.utils

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import java.net._
import java.io._

object SetData
{
  
  Configgy.configure("farq.cfg")
  
  def sendSet( args:Array[String]) =
  {

    
    var data = args(0).getBytes
    
    var s = new Socket("127.0.0.1", 9999)
    
    var out = s.getOutputStream
    
    var dos = new DataOutputStream( out )
    
    var request = "SETMYKEY|".getBytes
    
    dos.write( request )
   
    dos.writeInt( data.length )
    
    dos.write( data, 0, data.length )
    

    s.close()    
  }

 
  
  def main(args: Array[String]) =
  {

    sendSet( args)

  }
}


