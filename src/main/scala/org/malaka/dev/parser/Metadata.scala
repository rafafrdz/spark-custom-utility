package org.malaka.dev.parser

private [parser] object Metadata {

  val LOGICALFORMAT = "logicalFormat"

  /**
   * date pattern of the field (to parse dates in string)
   */
  val FORMAT = "format"

  /**
   * locale of the field (to parse dates in string or numerics in string)
   */
  val LOCALE = "locale"

  /**
   * autorename field flag
   */
  val RENAME = "rename"

  /**
   * flag to specify if the field is token (or must be token)
   */
  val TOKENIZED = "tokenized"

  /**
   * tokenization method used in the field
   */
  val TOKENIZATIONMETHOD = "tokenizationMethod"

  /**
   * origin for tokenization
   */
  val ORIGIN = "origin"

  /**
   * flag to spacify if a field is optional in validation stage
   */
  val OPTIONAL = "optional"


  /**
   * This variable defines all posible parameters for metadatas.
   */
  val metadataParameters: Map[String, String] = Map(
    "logicalFormat" -> LOGICALFORMAT,
    "format" -> FORMAT,
    "locale" -> LOCALE,
    "rename" -> RENAME,
    "tokenized" -> TOKENIZED,
    "tokenizationMethod" -> TOKENIZATIONMETHOD,
    "origin" -> ORIGIN,
    "optional" -> OPTIONAL)

}
