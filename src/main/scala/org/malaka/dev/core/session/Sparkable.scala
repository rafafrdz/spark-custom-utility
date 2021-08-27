package org.malaka.dev.core.session

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.malaka.dev.core.logger.Logger
import org.malaka.dev.core.logger.show.LazyLogger

trait Sparkable
  extends Serializable {

  final val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  final lazy val sc: SparkContext = spark.sparkContext
  lazy val localProcess: Boolean = spark.sparkContext.getConf.get("spark.master").contains("local")

  private type LoggerType = LazyLogger
  def logger(implicit l: Logger[LoggerType]): Logger[LoggerType] = l

}