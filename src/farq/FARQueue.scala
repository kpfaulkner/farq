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

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.collection.mutable.Queue

import FARQ.Datatypes._

class FARQueue extends Actor
{

  // this is fraught with dangers....
  var idCount = 1
  
  
  val log = Logger.get

  // main write queue.
  var queue = new Queue[ Entry ]()
  
  // fallback read queue, incase reading to too slow.
  //var readQueue = new Queue[ Entry]()
  
  // last id of the entry returned.
  var lastReadId = 0
  
  // indicate if read queue should be used.
  // This should only be set to true if the reading of the queue isn't
  // being performed quick enough and a back log starts to happen.
  var useReadQueue = true
  
  // put here if we're trying to retrieve it.
  // will be invisible for a while, but then returned to visible 
  // if not deleted.
  // really a list.
  var invisibleQueue =  List[ Entry ]()
  
  val maxQueueSize = Integer.parseInt( Configgy.config.getString("queue_size", "200" ) )

  // used to bumping off content.
  var resizeFactor = Configgy.config.getString("resize_factor", "0.5").toFloat
  
  var persistQueue = new PersistQueue()
  
  def act()
  {
    loop
    {
      react
      {
        case ( FARQCommands.setCommand, entry:Entry) =>
        { 
          handleSet( entry )
  
          // bad form?
          sender ! new Status( StatusCodes.SET_SUCCESS )
          
        }
        
        case FARQCommands.getCommand =>
        {
          // get the latest.
          
          var e = handleGet()
          var statusCode = StatusCodes.GET_SUCCESS
          
          if ( e == null )
          {
            statusCode = StatusCodes.GET_EMPTY
          }
          
          // potentially returning a numm entry... sucky...
          sender ! ( new Status(  statusCode ), e)
        }
        
        case ( FARQCommands.delCommand, id:Int ) =>
        {
          // delete existing one.
          handleDel( id )
          sender ! new Status( StatusCodes.DEL_SUCCESS )
        }

      }
    }
  }
  
  // FIXME: Throw exception/error for failed add.
  def handleSet( entry:Entry ) =
  {
    log.info("FARQueue::handleSet start")
    
    //log.debug("entry is " + entry.toString() )
    //log.debug("entry key is " + entry.key )
    
    //log.debug("existing length is " + queue.length.toString() )
    
    entry.id = idCount
    idCount += 1
    
    // persist the entry to disk.
    var persisted = persistQueue.add( entry )
    
    if ( persisted )
    {
      // if using read queue, then dont need to add to memory queue, just add directly to
      // persist queue.
      if ( !useReadQueue )
      {
        // add to memory queue.
        queue += entry
      
      }
       
      // if memory queue too long, then trim a percentage.
      while ( queue.length > maxQueueSize * resizeFactor )
      {
        queue.dequeue
        
        // yeah yeah, reassigning too many times.
        useReadQueue = true
      }   
    }
    else
    {
      log.error("FARQueue::handleSet error. Cant persiste entry " + entry.id.toString() )
    }
    
  }


  def handleGet( ): Entry =
  {
    log.info("FARQueue::handleGet start")
    
    var entry:Entry = null
    
    try
    {
      // loop until we either get NO entry (empty queues) or we get an entry.id
      // that is greater than the lastReadId value
      var done = false
      //log.debug("lastReadId is " + lastReadId.toString() )
      while ( ! done )
      {
        entry = getNextEntry()
        
        if (( entry == null) || ( entry.id > lastReadId ))
        {
          
          if ( entry != null )
          {
            lastReadId = entry.id
          }
    
          done = true
        }
      }
      
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQueue::handleGet exception " + ex.toString() )
    } 
    
    if ( entry != null )
    {
      lastReadId = entry.id
    }
    
    return entry
  }

  // shift from real Q to invisible Q.
  def getNextEntry( ): Entry =
  {
    log.info("FARQueue::getNextEntry start")
    
    var entry:Entry = null
    
    try
    {
    
      // check which queue we should be using.
      if (  useReadQueue )
      {
        // try reading from readQueue
        // if empty, try and reload from persist queue.
        if ( queue.isEmpty )
        {
          // load from persist.
          // make sure that the queue loaded only has entry id's greater than lastReadId
          queue = persistQueue.loadOldestPersistedQueue( lastReadId )
        }
        
        if ( !queue.isEmpty)
        {
          entry = readQueue.dequeue
        }
        else
        {
          useReadQueue = false
        }
      }
      else
      {
        log.debug("not in read behind mode")
        
        // just use regular Q....
        if ( !queue.isEmpty )
        {
          //log.debug("queue size is " + queue.size.toString() )
          entry = queue.dequeue
          
          //log.debug("queue not empty, had entry " + entry.id.toString() )
        }
      }
      
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQueue::handleGet exception " + ex.toString() )
    } 
    
    
    return entry
  }


  // remove entry from invisible Q with given id.
  def handleDel( id:Int )  =
  {
    log.info("FARQueue::handleDel start")
    
    try
    {
      // yeah yeah, reconstructing list as opposed to just removing from existing list... but will give it a go.
      var newInvisibleQueue = invisibleQueue.filter( _.id != id )
      
      invisibleQueue = newInvisibleQueue
      
    }
    catch
    {
      case ex: Exception =>
        log.error("FARQueue::handleDel exception " + ex.toString() )
    } 
    

  }  
}
