package org.malaka.dev.core

import org.apache.spark.sql.DataFrame
import org.malaka.dev.core.malaka.dataframe.{MalakaDataFrame, MalakaDataFrameSession}
import org.malaka.dev.core.session.Sparkable

object implicits extends Sparkable {
  implicit class ImplicitsDataFrame(df: DataFrame) {
    def mk: MalakaDataFrame = MalakaDataFrame.from(df)
    def mlk: MalakaDataFrameSession = MalakaDataFrameSession.from(df)
  }

}
