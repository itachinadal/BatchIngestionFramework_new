package com.anurag.learning.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object FileSystemUtils {

  /*
  Provides a Singleton instance of Hadoop File system
   */
  val fetchFileSystem: FileSystem = FileSystem.get(new Configuration())
}
