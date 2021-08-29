package org.malaka.dev.core.malaka.dataframe

import org.apache.spark.sql.DataFrame
import org.malaka.dev.core.malaka.{MalakaSession, session}

import scala.language.implicitConversions

trait MalakaDataFrameSession extends AbstractMalakaDataFrame[session] {
  protected override implicit def conversion(d: DataFrame): MalakaSession = MalakaDataFrame.from(d)

  override protected def data: MalakaSession = conversion(df)
}

private[dev] object MalakaDataFrameSession {
  def from(dataframe: DataFrame): MalakaDataFrameSession = new MalakaDataFrameSession {
    val df: DataFrame = dataframe
  }

  def open(dataframe: DataFrame): MalakaDataFrameSession = from(dataframe)

}
