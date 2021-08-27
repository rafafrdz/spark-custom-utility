package org.malaka.dev.parser

import org.json4s.JValue
import org.json4s.jackson.JsonMethods

object Json {

  /**
   * Schemas could contain a Byte Order Mark (BOM) that is the Unicode char '\uFEFF'
   * This character breaks the json conversion and we need to remove it.
   *
   * @param content schema content in string
   * @return schema content without Byte Order Mark
   */
  private def removeBomCharacter(content: String): String = {
    content.substring(content.indexOf('{'))
  }

  /** Read json content from path
   *
   * @param json path of json
   * @return the JValue of the content path
   */
  def parse(json: String): JValue = JsonMethods.parse(removeBomCharacter(json))
  def from(url: String): JValue = {
    val json: String = ??? // get from Request
    parse(json)
  }

}
