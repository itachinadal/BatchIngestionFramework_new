package com.anurag.learning.spark.com.anurag.learning.spark.models

import java.util.UUID

import com.anurag.learning.spark.utils.Enums.RunType.RunType
import com.anurag.learning.spark.utils.Enums.SourceType.SourceType
import com.anurag.learning.spark.utils.Enums.{RunType, SourceType}
import com.anurag.learning.spark.utils.SdmConstants._
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}


case class SriParams(source: String, country: String, hdfsBaseDir: String, sriOpenSchema: String, sriNonOpenSchema: String, verifyTypesSchema: String, opsSchema: String, sriOpenPartitionColumn: String, sriNonOpenPartitionColumn: String, verifyTypesPartitionColumn: String, hdfsTmpDir: String, tableConfigXml: String,reconTableName: String ,reconTableNameColumn: String, reconCountColumn: String, runType: RunType, businessDate: String, batchPartition: String, eodMarker: String, sourcingType: SourceType, eodTableName: String, reconOutputTable: String, metadataSchema: String, recordTypeBatch: String, dateColumnFormat: String, timeStampCol: String, timestampColFormat: String, vTypesAuditColSchema: String, sriAuditColSchema: String, systemAuditColSchema: String, parallelExecution: Boolean = true, isTableLevelEod: Boolean = false, reconSourceQuery: String = "", isRerun: Boolean = false) {

  def getSriOpenPartitionpath(tableName: String, partition: String): String = {

    getSriOpenPath(tableName) + sriOpenPartitionColumn + "=" + partition + "/"
  }

  def getSriOpenPath(tableName: String): String = {

    hdfsBaseDir + sriOpenSchema + "/" + tableName + "/"
  }
  def getSriOpenTmpPath(tableName: String): String ={
    hdfsTmpDir + "/" + sriOpenSchema + "/" + tableName + "/"
  }
  def getSriNonOpenPartitionPath(tableName: String, partition: String): String={
    getSriNonOpenPath(tableName) + sriNonOpenPartitionColumn + "=" + partition + "/"
  }
  def getSriNonOpenPath(tableName: String): String ={
    hdfsBaseDir  + sriNonOpenSchema + "/" + tableName + "/"
  }
  def getSriNonOpenTmpPath(tableName: String): String = {
    hdfsTmpDir + "/" + sriNonOpenSchema + "/" + tableName + "/"
  }
  def getVerifyTypesPath(tableName: String): String = {
    hdfsBaseDir + verifyTypesSchema + "/" + tableName + "/"
  }
  def getVerifyTypesTmpPath(tableName: String): String = {
    hdfsTmpDir + "/" +verifyTypesSchema + "/" + tableName + "/"
  }
  def isCdcSource: Boolean = {
    sourcingType == SourceType.CDC
  }

  def isRecordTypeBatch: Boolean ={
    isBatchSource && recordTypeBatch.equalsIgnoreCase("Y")
  }
  def isBatchSource: Boolean = {
    (sourcingType == SourceType.BATCH_FIXEDWIDTH) || (sourcingType == SourceType.BATCH_DELIMITED)
  }
  def fullDump = if (runType == RunType.FULLDUMP) true else false
  def businessDt : DateTime = {
    Try(TimestampFormat.parseDateTime(businessDate)) match {
      case Success(bd) => bd
      case Failure(throwable: Throwable) =>
        //Allow it to blow up if both formats doesn't work
        DateFormat.parseDateTime(businessDate)
    }
  }
}

/*  Factory for SriParam */
object SriParams {
 def apply(paramConf: Configuration): SriParams = {
   apply(paramConf, "", "", "", "")
 }

  def apply(paramConf: Configuration, partition: String, businessDate: String, runType: String, eodMarker: String) = {

    val country: String = paramConf.get("edmhdpif.config.country")
    val source: String = paramConf.get("edmhdpif.config.source")
    val hdfsDataParentDir = paramConf.get("edmhdpif.config.hdfs.data.parent.dir") + "/"
    val sriOpenSchema = paramConf.get("edmhdpif.config.sri.open.schema")
    val sriNonOpenSchema = paramConf.get("edmhdpif.config.sri.nonopen.schema")
    val verifyTypesSchema = paramConf.get("edmhdpif.config.storage.schema")
    val opsSchema = paramConf.get("edmhdpif.config.ops.schema")
    val sriOpenPartitionName = paramConf.get("edmhdpif.config.sri.open.partition.name","ods")
    val sriNonOpenPartitionName = paramConf.get("edmhdpif.config.sri.nonopen.partition.name","nds")
    val verifyTypesPartitionName = paramConf.get("edmhdpif.config.storage.partition.name","vds")
    val dateColFormat: String = paramConf.get("edmhdpif.config.datecolformat")
    val timestampcol: String = paramConf.get("edmhdpif.config.timestampcol")
    val timestampColFormat : String = paramConf.get("edmhdpif.config.timestampcolformat")
    val hdfsTmpDir : String = paramConf.get("edmphdpif.config.hdfs.tmpdir","/tmp") + "/" + UUID.randomUUID().toString
    val tableConfigXml = paramConf.get("edmphdpif.config.hdfs.tableconfig.xml.path")
    val reconTable = paramConf.get("edmhdpif.config.recon.tablename","")
    val reconTableNameCol = paramConf.get("edmhdpif.config.recon.tablename.colname","")
    val reconTableCountCol = paramConf.get("edmphdpif.config.recon.tablecount.colname","")
    val recordTypeBatch = paramConf.get("edmphdpif.config.batch.recordtype","N")
    val vTypesAuditColSchema = paramConf.get("edmphdpif.config.vtypes.auditcolschema","")
    val sriAuditColSchema = paramConf.get("edmphdpif.config.sri.auditcolschema","")
    val systemAuditColSchema = paramConf.get("edmphdpif.config.system.auditcolschema","")
    val runTypeEnum = RunType.fromString(runType).getOrElse(RunType.INCREMENTAL)
    val sourcingType = SourceType.fromString(paramConf.get("edmphdpif.config.sourcing.type")).getOrElse(SourceType.CDC)
    val eodTableName = paramConf.get("edmphdpif.config.eod.table.name","eod_table")
    val parallelExecution = paramConf.get("edmphdpif.config.parallel.execution","true").toBoolean
    val isTableLevelEod = paramConf.get("edmphdpif.config.eod.marker.strategy", "global_eod").equalsIgnoreCase(TableLevelEod)
    val reconSourceQuery = paramConf.get("edmphdpif.config.recon.source.query", "")
    val reconOutputTableName = paramConf.get("edmphdpif.config.recon.output.tablename","recon_output")
    val metadataTableName = paramConf.get("edmphdpif.config.commonmeta.schema",s"${source}_comman_metadata")
    new SriParams(source, country, hdfsDataParentDir, sriOpenSchema, sriNonOpenSchema, verifyTypesSchema, opsSchema, sriOpenPartitionName, sriNonOpenPartitionName, verifyTypesPartitionName, hdfsTmpDir, tableConfigXml, reconTable, reconTableNameCol, reconTableCountCol, runTypeEnum, businessDate, partition, eodMarker, sourcingType, eodTableName, reconOutputTableName, metadataTableName, recordTypeBatch, dateColFormat, timestampcol, timestampColFormat, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, parallelExecution, isTableLevelEod, reconSourceQuery)
  }
}
