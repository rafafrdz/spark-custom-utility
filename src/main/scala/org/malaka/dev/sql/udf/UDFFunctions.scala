package org.malaka.dev.sql.udf

import org.malaka.dev.core.session.Sparkable

object UDFFunctions extends Sparkable {
  //  def build[@specialized (Int, Char, Boolean) A: ClassTag, @specialized (Int, Char, Boolean) B: ClassTag](anoyFunc: A => B): UserDefinedFunction = udf(anoyFunc)
  //  def applyUDF[@specialized (Int, Char, Boolean) A: ClassTag, @specialized (Int, Char, Boolean) B: ClassTag](anoyFunc: A => B)(field: String): Column =
  //    build(anoyFunc)(implicitly[ClassTag[A]], implicitly[ClassTag[B]])(col(field))
  //  def applyUDF(udfCustom: UserDefinedFunction)(field: String): Column = udfCustom(col(field)).as(field)
  //  def applyUDF(udfCustom: UserDefinedFunction)(field: String, dataType: DataType): Column = udfCustom(col(field)).cast(dataType).as(field)

}
