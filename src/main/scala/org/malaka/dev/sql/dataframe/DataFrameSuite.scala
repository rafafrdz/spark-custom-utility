package org.malaka.dev.sql.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.malaka.dev.core.malaka.dataframe.MalakaDataFrame
import org.malaka.dev.core.session.Sparkable
import org.malaka.dev.sql.column.ColumnSuite

import scala.util.Try

object DataFrameSuite extends Sparkable {

  type SetColumn = (String, Column)

  def toMalaka(df: DataFrame): MalakaDataFrame = MalakaDataFrame.from(df)

  def getColumns(df: DataFrame): Array[Column] = df.columns.map(df(_))

  def hasColumn(df: DataFrame, colName: String): Boolean = Try(df(colName)).isSuccess

  def adaptSchema(df: DataFrame, schema: StructType, cast: Boolean = false): DataFrame = {
    val cols: Array[Column] = ColumnSuite.from(df, schema, cast)
    df.select(cols: _*)
  }

  def dropColumn(df: DataFrame, colName: String): DataFrame = {
    val restCol: Array[Column] = df.columns.filterNot(cn => cn == colName).map(df(_))
    df.select(restCol: _*)
  }

  def addColumn(df: DataFrame, setCol: SetColumn*): DataFrame = {
    val setColAlias: Seq[Column] = setCol.map { case (colName, col) => col.alias(colName) }
    val allCols: Array[Column] = getColumns(df) ++ setColAlias
    df.select(allCols: _*)
  }

  def addColumn(df: DataFrame, colName: String, col: Column): DataFrame = {
    val restCols: Array[Column] = getColumns(df)
    df.select(restCols :+ col.alias(colName): _*)
  }

  def addNullColumn(df: DataFrame, colName: String, dataType: DataType = StringType): DataFrame =
    addColumn(df, colName, ColumnSuite.nullCol(colName, dataType))


  private val IDFIELD: String = "idx"

  /**
   * Función que genera una columna indice incremental.
   * En el caso de que ya exista esa columna, se pisa y se genera la nueva columna indice.
   *
   * @param df DataFrame Input
   */

  def addIncrementalId(df: DataFrame): DataFrame = {
    lazy val dfNoId: DataFrame = dropColumn(df, IDFIELD)
    if (hasColumn(df, IDFIELD)) assignId(dfNoId) else assignId(df)
  }

  /**
   * Funcion que genera una columna indice incremental
   *
   * @param df DataFrame Input
   */

  private def assignId(df: DataFrame): DataFrame = {
    val incrementalField: StructField = StructField(IDFIELD, IntegerType, false)
    val finalSchema: StructType = df.schema.add(incrementalField)
    val rowId: RDD[Row] = df.rdd.zipWithUniqueId().map { case (row, indx) =>
      val finalSeq: Seq[Any] = row.toSeq :+ indx.toInt
      new GenericRowWithSchema(finalSeq.toArray, finalSchema)
    }
    spark.createDataFrame(rowId, finalSchema)
  }

  private val COUNTFIELD: String = "cnt"

  def findDuplicates(df: DataFrame, colName: String*): DataFrame = {
    val toFind: Seq[Column] = colName.map(df(_))
    df.groupBy(toFind: _*)
      .agg(count("*").alias(COUNTFIELD))
      .filter(col(COUNTFIELD) > 1)
      .select(toFind: _*)
  }

  def safeJoin(left: DataFrame, right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame = {
    val leftCols: Array[Column] = getColumns(left)
    val rightColsNoLeft: Array[Column] = ColumnSuite.diff(right, left)
    val expr: Column = ColumnSuite.eqNullSafeExpr(left, right, usingColumns: _*)
    left.join(right, expr, joinType).select(leftCols ++ rightColsNoLeft: _*)
  }
}
