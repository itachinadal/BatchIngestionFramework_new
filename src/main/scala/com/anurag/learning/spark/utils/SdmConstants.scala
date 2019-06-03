package com.anurag.learning.spark.utils

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object SdmConstants {

  val TableLevelEod = "table_level_eod"
  val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
  val TimestampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  lazy val formatter: String => DateTimeFormatter = { str : String => DateTimeFormat forPattern str }
  val PrefixConfigEdmHdpIfTable = "edmhdpif.table."
  val SuffixConfigColSchema = "col_schema"
  val SuffixConfigKeyCols = ".keycols"
  val SuffixConfigSourceType = ".sourcetype"
  val SuffixConfigReconQuery = ".query"
  val SuffixConfigDeleteValue = ".deletevalue"
  val SuffixConfigDeleteIndex = ".deleteindex"
  val SuffixConfigDeleteFieldName = ".deletefield"
  val SuffixConfigOperationTypeCol = ".operationaltypecol"
  val SuffixConfigInsertValue = ".insertvalue"
  val SuffixConfigBeforeValue = ".beforevalue"
  val SuffixConfigAfterValue = ".aftervalue"
  val SuffixConfigRunType = ".runtype"
  val ReconFileRename = ".reconfilerename"

}
