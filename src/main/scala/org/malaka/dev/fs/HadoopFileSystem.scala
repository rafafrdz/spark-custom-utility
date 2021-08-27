package org.malaka.dev.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.malaka.dev.core.session.Sparkable

private[fs] trait HadoopFileSystem {
  self: Sparkable =>
  private lazy val hadoopConf: Configuration = sc.hadoopConfiguration
  protected final def fs: FileSystem = FileSystem.get(hadoopConf)
  protected final def hdfs(path: String) = new Path(path)

}
