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
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream


import FARQ.Datatypes._

// identifies file to read, and deletes older/used files.
object FileIdentifier
{
  val log = Logger.get
  
  // queue sizes.
  var cacheDir = Configgy.config.getString("queue_dir", "cache" )

  val usedExtension = ".used"
  
  // rename used files upon loading.
  // since any used that that still exists during loading mightn't have been fully used
  // so need to rename to make sure they'll get used to populate the queues again.
  renameAllUsedFiles()
  
  def renameAllUsedFiles() =
  {
    log.info("FileIdentier::renameAllUsedFiles start")
    var f = new File( cacheDir )
    
    var fileArray = f.listFiles()
    
    var fileList = fileArray.toList
    
    for ( file <- fileList )
    {
      var fn = file.getPath()
      if ( fn.endsWith( usedExtension ))
      {
        // remove .used from name.
        var newName = fn.substring(0, fn.length- usedExtension.length)
        new File( fn ).renameTo( new File( newName ) )
      }
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
    
    var nonUsedFileList = fileList.filter( x=> !x.getPath().contains("used"))
    if ( nonUsedFileList.length > 0 )
    {
      fn = nonUsedFileList(0).getPath()
    }
    
    log.debug("oldest filename " + fn )
    return fn
    
  }
  
  def  getOldestDestroyUsed( oldFile:String ): String =
  {
    // oldFile passed in is a queue name that has been used and finished with
    // so delete the file.
     
    if ( oldFile != "")
    {
       var fn2 = "./"+oldFile
       
      // need to close streams?
      new File( fn2 ).delete
    }
    
    // find oldest file.
    var filename = getOldestFilename()
    
    var newFileName = filename + usedExtension
    // rename file so it wont be picked up next time.
    new File( filename ).renameTo( new File( newFileName ) )
    
    return newFileName
    
  }

}

class PersistQueue
{

  val log = Logger.get
  
  // queue sizes.
  var cacheDir = Configgy.config.getString("queue_dir", "cache" )
  var persistQueueSize = Configgy.config.getInt("persist_queue_size", 1000000 )
  
  // for writing. duh
  var outFileStream:FileOutputStream = null
  var dataOutputStream:DataOutputStream = null
  
  // for reading. double duh.
  var inFileStream:FileInputStream = null
  
  var fileSize = 0

  var currentReadFile = ""
  var currentWriteFile = ""
  
  //openNewStream()
  
  
  def openStreamForReading( fn: String) =
  {
    inFileStream = new FileInputStream( fn )
  
  }
  
  
  // open new stream...   how to determine filename?
  def openNewStream( ) =
  {
    var fn = cacheDir + "/" + System.nanoTime().toString()
    currentWriteFile = fn
    
    // true param for allowing appending.
    outFileStream = new FileOutputStream( fn, true )
    dataOutputStream = new DataOutputStream( outFileStream )
    fileSize = 0
  }

  // closes open file for writing,
  // may do more in future.
  def roll( ) =
  {
    outFileStream.close()
    dataOutputStream.close()
    dataOutputStream = null // FIXME
  }
  
  def add( entry: Entry ):Boolean =
  {
    log.info("PersistQueue::add start")
    
    // add to existing OPEN file stream (need to check about reopening if required)
    if (dataOutputStream == null )
    {
      openNewStream()
      
    }
    // write ID of entry.
    dataOutputStream.writeInt( entry.id )
    
    // Write length of data, then data itself.
    dataOutputStream.writeInt( entry.data.length)
    
    // write data itself
    dataOutputStream.write( entry.data )
    
    fileSize += entry.data.length
    
    return true
    
  }
  
  def closeAllStreams() =
  {
    if ( outFileStream != null )
    {
      outFileStream.close()
      dataOutputStream.close()
      dataOutputStream = null
    }
    
    if ( inFileStream != null )
    {
      inFileStream.close()
    }
    
  }
  
  def loadOldestPersistedQueue( lastReadId:Int ): Queue[Entry] =
  {
    
    log.info("PersistQueue:loadOldestPersistedQueue start")
    
    var q = new Queue[ Entry ]()
    var done = false
    //var fn = getOldestFilename()
    var fn = FileIdentifier.getOldestDestroyUsed( currentReadFile )
    currentReadFile = fn
    
    var previousFileName = ""
    while ( ( fn != "") && (!done) )
    {
      //log.debug("fn is "+ fn )
      
      var inFileStream = new FileInputStream( fn )
      var dis = new DataInputStream( inFileStream )
      
      try
      {
        while ( !done )
        {
         //log.debug("reading data")
         var entryId = dis.readInt()
         var length = dis.readInt()
         
         var buffer = new Array[Byte]( length )
         
         dis.read( buffer, 0, length )
         
         // only want ones we haven't delivered.
         if ( entryId > lastReadId )
         {
           //log.debug("appending to q")
           var entry = new Entry("DUMMY")
           entry.id = entryId
           entry.data = buffer
           q += entry
         }
       }
       
  
      }
      catch
      {
        case ex: Exception =>
          log.debug("FARQHandler::loadOldestPersistedQueue end of file" )
      }
      
      //log.debug("queue length is " + q.size.toString() )
      
      // make sure streams are closed.
      dis.close()
      inFileStream.close()
      previousFileName = fn
      
      // if we get here and the q is empty it means the entire persist file only has old information
      // which means it can be deleted.
      // inefficient, but will do for now until proven otherwise.
      // HACK HACK HACK HORRIBLE STUFF.
      if ( q.isEmpty )
      {
        fn = FileIdentifier.getOldestDestroyUsed( currentReadFile )
        currentReadFile = fn
      
        // no file name means nothing to do.
        if (fn == "")
        {
          closeAllStreams()
          done = true
        }
        
        
      }
      else
      {
        // have data from a file.
        done = true
      }
    }
    return q
    
  }
  
  
}
