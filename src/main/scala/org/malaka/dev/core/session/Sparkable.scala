package org.malaka.dev.core.session

import org.apache.spark.sql.SparkSession

trait Sparkable
  extends Serializable with LazyLogger {
  self =>
  lazy val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  lazy val localProcess: Boolean = spark.sparkContext.getConf.get("spark.master").contains("local")


}