package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object LoggerLevels  extends Logging{

  def setStreamingLogLevels(): Unit ={
    val log4jInit = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!log4jInit){
      println("init logging...................................")
      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }
}
