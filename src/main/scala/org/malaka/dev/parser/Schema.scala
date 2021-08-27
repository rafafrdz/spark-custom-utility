package org.malaka.dev.parser

import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JBool, JNothing, JNull}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, JObject, JString, JValue, NoTypeHints}

object Schema {

  def from(url: String, includeDeleted: Boolean = false, includeMetadata: Boolean = false): StructType = {
    val json: JValue = Json.from(url)
    from(json, includeDeleted, includeMetadata)
  }

  def from(json: JValue, includeDeleted: Boolean = false, includeMetadata: Boolean = false): StructType =
    toSqlType(json, includeDeleted, includeMetadata)

  /**
   * This function takes an avro schema and returns a sql schema.
   *
   * @param json         json object representation of avro schema
   * @param includeDeleted  use true if sql schema retrieved must include deleted fields
   * @param includeMetadata use true if sql schema retrieved must include metadata fields
   * @return
   */
  private def toSqlType(json: JValue, includeDeleted: Boolean = false, includeMetadata: Boolean = false): StructType =
    parse(json, includeDeleted, includeMetadata).asInstanceOf[StructType]

  private def parse(json: JValue, includeDeleted: Boolean = false, includeMetadata: Boolean = false): DataType =
    json match {
      case JString(t) => typeToCast(t)
      case _ => json \ "type" match {
        case JString("record") => StructType(extractFields(json, includeDeleted, includeMetadata))
        case JString("array") => ArrayType(parse(json \ "items", includeDeleted, includeMetadata))
        case JString("map") => MapType(StringType, parse(json \ "values", includeDeleted, includeMetadata))
        case JArray(types: Seq[JValue]) => parse(types.filter(_ != JString("null")).head, includeDeleted, includeMetadata)
        case JString(other) => typeToCast(other)
        case obj: JObject => parse(obj, includeDeleted, includeMetadata)
      }
    }

  private def extractFields(json: JValue, includeDeleted: Boolean = false, includeMetadata: Boolean = false): List[StructField] = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    json \ "fields" match {
      case JArray(fields) => fields.
        filter(field => includeDeleted || !(field \ "deleted").extractOpt[Boolean].getOrElse(false)).
        filter(field => includeMetadata || !(field \ "metadata").extractOpt[Boolean].getOrElse(false)).
        map(field => {
          val name: String = (field \ "name").extract[String]
          val dataType: DataType = parse(field)
          StructField(name, dataType, isNullable(field), createMetadata(field))
        })
    }
  }

  private def createMetadata(field: JValue): Metadata = {
    import Metadata._
    val metadata = new MetadataBuilder()
    val metadataMap = metadataParameters
    metadataMap.map {
      case (schemaKey, metadataKey) =>
        field \ schemaKey match {
          case JString(v) => metadata.putString(metadataKey, v)
          case JBool(v) => metadata.putBoolean(metadataKey, v)
          case JNothing | JNull =>
        }
    }
    metadata.build
  }

  private def isNullable(field: JValue): Boolean = field \ "type" match {
    case JArray(types: Seq[JValue]) if types.contains(JString("null")) => true
    case _ => false
  }

  /**
   * Hardcoded parsed file from different formats.
   *
   * @param dataType format from the schema file
   * @return Spark data type.
   */
  private def typeToCast(dataType: String): DataType = {
    val decimalMatcher = """^decimal *\( *(\d+) *, *(\d+) *\)$""".r
    val decimalOnlyPrecisionMatcher = """^decimal *\( *(\d+) *\)$""".r

    dataType.trim.toLowerCase match {
      case "string" => StringType
      case "int32" | "int" | "integer" => IntegerType
      case "int64" | "long" => LongType
      case "double" => DoubleType
      case "float" => FloatType
      case "boolean" => BooleanType
      case "date" => DateType
      case "timestamp_millis" => TimestampType
      case "timestamp" => TimestampType
      case decimalMatcher(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case decimalOnlyPrecisionMatcher(precision) => DecimalType(precision.toInt, 0)
      case other: String => throw new scala.Exception("Problema casteando en la lectura del schema")
    }
  }

}
