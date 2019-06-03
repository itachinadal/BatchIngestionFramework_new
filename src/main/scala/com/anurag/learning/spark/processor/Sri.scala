package com.anurag.learning.spark.processor

import com.anurag.learning.spark.com.anurag.learning.spark.models.SriParams
import com.anurag.learning.spark.utils.FileSystemUtils
import com.anurag.learning.spark.utils.SriUtils.{getBusinessDayPartition, getParamConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeRow}
import org.apache.spark.sql.types.{DateType, DecimalType, Metadata, StringType, StructField, StructType, TimestampType}
import org.joda.time.DateTime

import scala.collection.mutable


/*
Entry Point for the Source Image construction process
*/
object Sri {

  val logger: Logger = Logger.getLogger(Sri.getClass)
  def main( args: Array[String]): Unit = {
    val nArgs = 5
    if(args.length != nArgs ){
      logger.error("Usage: Sri<eod Marker> <businees Day> <parameter xml hdfs path> <fulldump|incremental> <comma separated rerun tables>")
      sys.error("Usage: Sri<eod Marker> <businees Day> <parameter xml hdfs path> <fulldump|incremental> <comma separated rerun tables>")
      System.exit(1)
    } else {
      logger.info(s"Start time if the Job ${DateTime.now()}")
      logger.info("Spark Driver arguments - " + args.mkString(" | "))


      val sparkConf = new SparkConf()
        .registerKryoClasses(
          Array(
            classOf[mutable.WrappedArray.ofRef[_]],
          classOf[RowCount],
          classOf[Array[RowCount]],
          classOf[SriParams],
          classOf[Array[Row]],
          Class.forName("com.databricks.spark.avro.DefaultSource$SerializableConfiguratiom"),
          Class.forName("scala.reflect.ClassTag$$anon$1"),
          Class.forName("org.apache.spark.sql.types.StringType$"),
          Class.forName("scala.collection.immutable.MAp$EmptyMap$"),
          classOf[Array[InternalRow]],
          classOf[Array[StructField]],
          classOf[Array[Object]],
          classOf[Array[String]],
          classOf[StructField],
          classOf[Metadata],
          classOf[StringType],
          classOf[DecimalType],
          classOf[TimestampType],
          classOf[DateType],
          classOf[UnsafeRow],
          classOf[GenericRowWithSchema],
          classOf[StructType],
          classOf[OpsEvent],
          classOf[Array[OpsEvent]],
          classOf[ProcessMetadataEvent],
          classOf[Array[ProcessMetadataEvent]],
          Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
          Class.forName("org.apache.spark.sql.execution.LongHashedRelation"),
          Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
          Class.forName("org.apache.spark.execution.columnar.CachedBatch"),
          Class.forName("org.apache.spark.sql.execution.datasource.FileFormatWriter$WriteTaskResult"),
          classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
          classOf[org.apache.spark.unsafe.types.UTF8String],
          Class.forName("[[B")
        )
      ).set("spark.serializer","org.apache.serializer.KryoSerializer")
        .set("mapreduce.fileoutputcommitter.algorithm.version","2")
        .set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")

      val sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .appName("SriProcess_0.1")
        .enableHiveSupport()
        .getOrCreate()
      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool","production")

      val (todayPartition, paramPath, sriParams, tableConfigs, rerunTables, paramConf) = parseParameters()
      BootstrapProcessor.busineesDateProcess(todayPartition, sriParams, tableConfigs, rerunTables, paramConf)(sparkSession)
    }

    /*
    *Parses parameters and submits the Sri Job
     */
    def parseParameters() = {

      val Array(eodMarker, submitTime, paramPath, runType, rerunTableList) = args
      val fs: FileSystem = FileSystemUtils.fetchFileSystem
      val paramConf: Configuration = getParamConf(fs, paramPath)
      val todaysPartition: String = getBusinessDayPartition(submitTime,paramConf.get("edmhdpif.config.timestampcolformat")) match {
        case Some (part) => part
        case None => throw new scala.Exception(" Business date and todays partition format were incorrectly specified! ")
      }

      val sriParams: SriParams = SriParams(paramConf, todaysPartition, todaysPartition, runType, eodMarker)
      val tableConfigsFromParamFile = parseXmlConfig(paramPath, fs)
      val tableConfigs = parseXmlConfig(sriParams.tableConfigXml, fs) ++ tableConfigsFromParamFile
      val rerunTables: List[String] = RerunProcessor.getRerunTableList(rerunTableList, fs, tableConfigs)
      val rerunFlag = rerunTableList.nonEmpty
      val tableDictionary = if (rerunTables.isEmpty) tableConfigs else tableConfigs.filter(x => rerunTables.contains(x.name))
      (todaysPartition, paramPath, sriParams.copy(isRerun = rerunFlag), tableDictionary, rerunTables, paramConf)
    }
  }


}
