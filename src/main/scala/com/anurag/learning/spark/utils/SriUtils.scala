package com.anurag.learning.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import com.anurag.learning.spark.utils.SdmConstants._

import scala.util.Try



object SriUtils {

  val logger: Logger = Logger.getLogger(SriUtils.getClass)
  def getParamConf(fs: FileSystem, xmlPath: String): Configuration = {
    val xmlHdfs = new Path(xmlPath)
    val paramConf: Configuration = new Configuration()
    paramConf.addResource(fs.open(xmlHdfs), xmlPath)
    paramConf
  }

  def getBusinessDayPartition(submitTime: String, timestampColFormat: String): Option[String] = {

    val businessDay = Try(formatter(timestampColFormat) parseDateTime submitTime ).toOption
    businessDay.map(DateFormat.print)
  }


}
