package com.anurag.learning.spark.utils

object Enums {

  /*
  ** Defines whether a table is type Delta or Transaction
   */

  object TableType extends Enumeration {
    type TableType = Value
    val DELTA, TXN = Value

    def fromString(tableType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(tableType))
  }


  /*
   Defines whether a files of a table is to be treated as Fulldump/Snapshot or Incremental data
   */
  object RunType extends Enumeration {

    type RunType = Value
    val FULLDUMP, INCREMENTAL = Value
  def fromString(runType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(runType))
  }

  /*
  * Defines whether the source application has CDC generated files, FIXEDWIDTH files or plain DELIMITED files
  * */
  object SourceType extends Enumeration {

    type SourceType = Value
    val CDC, BATCH_FIXEDWIDTH, BATCH_DELIMITED = Value

    def fromString(sourceType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(sourceType))
  }
}
