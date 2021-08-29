package org.malaka.dev.core.malaka.dataframe

import org.apache.spark.sql.DataFrame
import org.malaka.dev.core.malaka.identity

import scala.language.implicitConversions

trait MalakaDataFrame extends AbstractMalakaDataFrame[identity] {
  protected override implicit def conversion(d: DataFrame): identity[DataFrame] = d

  override val df: DataFrame = data
}

private[dev] object MalakaDataFrame {
  def from(dataframe: DataFrame): MalakaDataFrame = new MalakaDataFrame {
    protected def data: DataFrame = dataframe
  }

  def openSS(dataframe: DataFrame): MalakaDataFrameSession = new MalakaDataFrameSession {
    val df: DataFrame = dataframe
  }

}
