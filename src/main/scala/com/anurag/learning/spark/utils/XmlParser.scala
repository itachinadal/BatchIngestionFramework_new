package com.anurag.learning.spark.utils

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import com.anurag.learning.spark.utils.SdmConstants._

import scala.collection.JavaConverters._
import scala.util.Try


object XmlParser {

  /*
  ** Given a table config, returns a collection of [[ColumnConfig]] that represents the columns in that table
   */
  def fetchColumnConfig(tableConfig: TableConfig): List[ColinmConfig] = getColumnConfigs(tableConfig.colSchema, "\\^")

  /*
  Given the column configs string for a table , returns a collection of [[ColumnConfig]] that represents the columns in table
   */
  def getColumnConfigs(colsSchemas: String, delimiter: String): List[ColumnConfig] = {
    if (StringUtils.isBlank(colsSchemas)) List()
    else {
      colsSchemas.split(delimiter).foldRight(Nil: List[ColumnConfig]) {
        (colSchema: String, list: List[ColumnSchema]) =>
          getColumnConfigFromColSchema(colSchema) :: list
      }
    }
  }


  /* Given a single  config string for a single column, returns the [[ColumnConfig]] scala binding */

  private def getColumnConfigFromColSchema(colSchema: String): ColumnConfig = {
    val splitValues: Array[String] = colSchema.split(" ")
    val width = if (splitValues.length > 2 && splitValues(2).equals("WIDTH"))
      splitValues(3) else null
    val nullable: Boolean = {
      if (null != width) if (splitValues.length > 4 && splitValues(4).equals("NOT")) false else true
      else if (splitValues.length > 2 && splitValues(2).equals("NOT")) false else true
    }

    val comment = if (splitValues(splitValues.length - 2).equals("COMMENT"))
      splitValues(splitValues.length - 1) else null

    ColumnConfig(splitValues(0), splitValues(1), nullable, width, comment)
  }

  /*Returns the list of primary key columns in that table */

  def fetchKeyColsFromTable(tableConfig: TableConfig): List[String] = {
    if (StringUtils.isNotBlank(tableConfig.keyCols)) {
      tableConfig.keyCols.split(",").map(_.trim).toList
    }
    else
      List()

  }

  /*Returns the list of columns in that table
    **/

  def fetchColNamesFromTable(tableConfig: TableConfig): List[String] = {
    fecthColumnConfig(tableConfig).map(_.name)
  }

  /**/

  def getConf(fs: FileSystem, path: String): Map[String, String] = {
    val hdfsPath = new Path(path)
    val patternR = (PrefixConfigEdmHdpIfTable).r
    if (fs.isDirectory(hdfsPath)) {
      fs.listStatus(hdfsPath).filter(_.getPath.getName.endsWith("xml")).map(t => {
        SriUtils.getParamConf(fs, t.getPath.toString).getValByRegex(patternR.toString()).asScala
      }).flatten[(String, String)].toMap
    }
    else {
      SriUtils.getParamConf(fs, path).getValByRegex(patternR.toString()).asScala.toMap
    }

  }

  /*Parses the table config xml and constructs the corresponding [[TableConfig]] scala binding */
  def parseXmlConfig(path: String, fs: FileSystem): List[TableConfig] = {
    val patternR = (SdmConstants.PrefixConfigEdmHdpIfTable + "(.*)" + SdmConstants.SuffixConfigColSchema).r
    val parseTableName: String => String = {
      case patternR(name) => name
    }
    val tables = getConf(fs, path)
    val tableNames: Map[String, String] = tables.filterKeys(matches(SdmConstants.PrefixConfigEdmHdpIfTable + "(.*)" + SdmConstants.SuffixConfigColSchema))
    tableNames.keys.foldRight(Nil: List[TableConfig]) {
      (key: String, tableList: List[TableConfig]) =>
        val tableName: String = parseTableName(key)
        val sourceType: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigSourceType, null)
        val keyCols: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigKeyCols, null)
        val colSchema: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigColSchema, null)
        val reconQuery: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigReconQuery, null)
        val deleteValue: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteValue, null)
        val deleteField: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteFieldName, null)
        val operationTypeCol: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigOperationTypeCol, null)
        val insertValue: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigInsertValue, null)
        val beforeValue: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigBeforeValue, null)
        val afterValue: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigAfterValue, null)
        val runType: Option[String] = tables.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigRunType)
        val reconFileName: String = tables.getOrElse(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.ReconFileRename, null)
        val deleteIndex: String = if (deleteField.nonEmpty) {
          (colSchema.split("\\^").map(_.split(" ")(0)).indexOf(deleteField) + 1).toString
        }
        else {
          (Try(tables.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteIndex).get.toInt).getOrElse(-2) + 1).toString
        }
        TableConfig(tableName, sourceType, keyCols, colSchema, reconQuery, deleteIndex, deleteValue, operationTypeCol, insertValue, beforeValue, afterValue, runType, reconFileName) :: tableList


    }

  }

  /* Construct a List of [[AllTabColumn]] for a source-country given a [[TableConfig]]*/
  def getAllTab

}
  /*Represents an entry of versioned TableConfig in a flat format  */

  case class AllTabColumn(tableName: String, ColumnName: String, country: String, source: String, )

