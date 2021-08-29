package org.malaka.dev.core.malaka.dataframe

import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import org.malaka.dev.core.malaka.{Malaka, identity, session}
import org.malaka.dev.sql.dataframe.DataFrameSuite

import scala.language.implicitConversions

private [dev] trait AbstractMalakaDataFrame[F[_]] extends Malaka[F, DataFrame] {
  val df: DataFrame
  protected implicit def conversion(d: DataFrame): F[DataFrame]
  def hasColumn(colName: String): Boolean = DataFrameSuite.hasColumn(df, colName)
  def columns: Array[Column] = DataFrameSuite.getColumns(df)
  def adaptSchema(schema: StructType, cast: Boolean = false): F[DataFrame] = DataFrameSuite.adaptSchema(df, schema, cast)
  def dropColumn(colName: String): F[DataFrame] = DataFrameSuite.dropColumn(df, colName)
  def addColumn(colName: String, col: Column): F[DataFrame] = DataFrameSuite.addColumn(df, colName, col)
  def withColumn(colName: String, col: Column): F[DataFrame] = addColumn(colName, col)
  def addNullColumn(colName: String, dataType: DataType = StringType): F[DataFrame] = DataFrameSuite.addNullColumn(df, colName, dataType)
  def withNullColumn(colName: String, dataType: DataType = StringType): F[DataFrame] = addNullColumn(colName, dataType)
  def withIndex: F[DataFrame] = DataFrameSuite.addIncrementalId(df)
  def addIndex: F[DataFrame] = withIndex
  def findDuplicates(colName: String*): F[DataFrame] = DataFrameSuite.findDuplicates(df, colName:_*)
  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): F[DataFrame] = DataFrameSuite.safeJoin(df, right, usingColumns, joinType)

  //  def adaptSchema

}
