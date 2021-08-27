package org.malaka.dev.sql.types

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.malaka.dev.core.session.Sparkable

import scala.annotation.tailrec
import scala.language.higherKinds

object Complex extends Sparkable {

  private def explodeStruct(struct: List[StructField]): List[StructField] = {
    @tailrec
    def aux(sss: List[StructField], xs: List[StructField]): List[StructField] = sss match {
      case Nil => xs.reverse
      case ::(head, tl) => head.dataType match {
        case ArrayType(elementType, _) => aux(StructField(head.name, elementType) +: tl, xs)
        case StructType(fields) => aux(fields.toList ++ tl, xs)
        case _ => aux(tl, head +: xs)
      }
    }

    aux(struct, Nil)
  }

  private def explodeStruct(struct: StructType): StructType = StructType(explodeStruct(struct.toList))

  def explodeSchema(df: DataFrame): StructType = explodeStruct(df.schema)

  def complexExplode(df: DataFrame, field: String): StructType = {
    val str1 = explodeSchema(df.select(field))
    val str2 = df.schema.filterNot(_.name == field)
    StructType(str2 ++ str1)
  }

  /** Explode manual para el campo jerarquico
   * Todo. In the future make it generic to be deeper on a complex field which contain
   * todo. other complex field and so on; and be deeper on multiple fields */

    private val lambdaRow = (row: Row, field: String) =>
      row.getSeq[Row](row.fieldIndex(field)).map { r =>
        val lengthRow: Int = r.length
        (0 until lengthRow).map(i => r.getAs[Any](i))
      }
  def manualExplode(df: DataFrame, field: String): DataFrame = {
    val restFields: Array[String] = df.schema.fieldNames.filterNot(_ == field)
    val schemaExploded: StructType = complexExplode(df, field)
    val explode: RDD[Row] = ??? // df.rdd.flatMap(row => lambdaRow(row,field))
    ???
  }

  def flatten[T, A](l: IndexedSeq[T]): IndexedSeq[A] = l flatMap {
    case ls: IndexedSeq[_] => flatten(ls)
    case h: A => IndexedSeq(h)
  }

  def flatten[T, A, B](l: IndexedSeq[T], f: A => B): IndexedSeq[B] = l flatMap {
    case ls: IndexedSeq[_] => flatten(ls)
    case h: A => IndexedSeq(f(h))
  }

  def flatten[T, A](l: List[T]): List[A] = l flatMap {
    case ls: List[_] => flatten(ls)
    case h: A => List(h)
  }
//  def flattenAny[T](xs: List[T]): List[Any] = flatten[T,Any](xs)


}
