package org.malaka.dev.core

import org.apache.spark.sql.DataFrame
import org.malaka.dev.core.malaka.dataframe.MalakaDataFrame

package object malaka {
  private [malaka] type identity[A] = A
  private [malaka] type session[A] = MalakaDataFrame

  type MalakaSession = session[DataFrame]


}
