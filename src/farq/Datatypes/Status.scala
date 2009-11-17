/* Copyright Ken Faulkner 2009 */

package FARQ.Datatypes



object StatusCodes 
{
  val SUCCESS = 0
  val FAILED = 1
  
  val SET_SUCCESS = 0
  val SET_FAILED = 1
  
  val GET_SUCCESS = 0
  val GET_FAILED = 1
  val GET_EMPTY = 2
  
  val DEL_SUCCESS = 0
  val DEL_FAILED = 1
  
  
}

class Status( c: Int )
{
  var code = c
  var msg = ""
  
}