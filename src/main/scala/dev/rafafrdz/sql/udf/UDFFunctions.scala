package dev.rafafrdz.sql.udf

import dev.rafafrdz.core.session.Sparkable
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

object UDFFunctions extends Sparkable {
//  def build[@specialized (Int, Char, Boolean) A: ClassTag, @specialized (Int, Char, Boolean) B: ClassTag](anoyFunc: A => B): UserDefinedFunction = udf(anoyFunc)
//  def applyUDF[@specialized (Int, Char, Boolean) A: ClassTag, @specialized (Int, Char, Boolean) B: ClassTag](anoyFunc: A => B)(field: String): Column =
//    build(anoyFunc)(implicitly[ClassTag[A]], implicitly[ClassTag[B]])(col(field))
//  def applyUDF(udfCustom: UserDefinedFunction)(field: String): Column = udfCustom(col(field)).as(field)
//  def applyUDF(udfCustom: UserDefinedFunction)(field: String, dataType: DataType): Column = udfCustom(col(field)).cast(dataType).as(field)

}
