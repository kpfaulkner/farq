package FARQ

import java.io.File
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.collection.mutable.Queue
import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream


import FARQ.Datatypes._

class PersistQueue
{

  val log = Logger.get

  // in memory Q.
  var queue = new Queue[ Entry ]()
  
  // queue sizes.
  val maxQueueSize = Integer.parseInt( Configgy.config.getString("queue_size", "200" ) )
  var cacheDir = Configgy.config.getString("queue_dir", "cache" )
  
  def add( entry: Entry ) =
  {
  
    if ( queue.length < maxQueueSize )
    {
      queue += entry
    }
    else
    {
      // store, clear then add.
      store()
      queue.clear()
      
      queue += entry
    }
    
  }
  
  def getOldestFilename( ) : String =
  {
  
    var fn = ""
    
    var f = new File( cacheDir )
    
    var fileArray = f.listFiles()
    
    var fileList = fileArray.toList
    
    // sort the sucker by modified time.
    fileList.sort( (a,b) => a.lastModified > b.lastModified ) 
    
    if ( fileList.length > 0 )
    {
      fn = fileList(0).getPath()
    }
    
    return fn
    
  }
  
  
  def getQueueBlock( ): Queue[Entry ] =
  {
    // get oldest block on disk.
    var fn = getOldestFilename()
    
    var q = new Queue[Entry]()
    
    if ( fn != "" )
    {
      q = retrieve( fn )
      
    } else
    {
      // return the in memory queue... since nothing is on disk.
      
      q = queue.clone()
      queue.clear()
      
    }
    
    return q
  }
  
  def store() =
  {
    log.info("PersistQueue::store start")
    
    try
    {
      // stores queue to disk.
      
      // hardcoded slash!!!
      var fn = cacheDir + "/" + System.nanoTime().toString()
      
      var outFile = new FileOutputStream( fn )
      var outStream = new ObjectOutputStream( outFile )
  
      outStream.writeObject( queue )
      outStream.close()

    }
    catch
    {
      case ex: Exception =>
        log.error("PersistQueue::store exception " + ex.toString() )
    } 
      
  }
  
  def retrieve( fn: String): Queue[ Entry ] =
  {
    // loads from file.
    log.info("PersistQueue::retrieve start")

    var q = new Queue[Entry]()
    
    try
    {

      var inFile = new FileInputStream( fn )
      var inStream = new ObjectInputStream( inFile )

      q = inStream.readObject().asInstanceOf[ Queue[Entry] ] 
      
      inStream.close()

      // delete it.
      var f = new File( fn )
      f.delete()
      
    }
    catch
    {
      case ex: Exception =>
        log.error("PersistQueue::retrieve exception " + ex.toString() )
    }   
    
    return q
    
  }
  
}
