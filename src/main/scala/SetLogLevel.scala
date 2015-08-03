package main.scala

import org.apache.log4j.{Level, Logger}

/**
 * Created by junius on 15-6-4.
 */
object SetLogLevel {

  def setLogLevel() = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      // logInfo("Setting log level to [WARN] for streaming example.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

  def main(args: Array[String]) {
    setLogLevel()
  }
}
