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
    log.info("PersistQueue::add start")   
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
    log.info("PersistQueue::getOldestFilename start")  
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
    log.info("PersistQueue::getQueueBlock start")
    
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
