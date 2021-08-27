package org.malaka.dev.io

import org.apache.hadoop.fs.FileStatus
import org.malaka.dev.core.session.Sparkable
import org.malaka.dev.io.FSystem.lsTree

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[io] object FStatus extends HadoopFileSystem with Sparkable{

  def trazeStatus(father: FileStatus, onlyFiles: Boolean = true): Array[String] =
    getChild(father) match {
      case None => Array(getPath(father))
      case Some(child) => lsTree(child, onlyFiles)
    }

  def traverseStatuses(statuses: Array[FileStatus],  onlyFiles: Boolean = true): Array[String] = {
    @tailrec
    def traverse(sts: Array[FileStatus], xs: Array[String]): Array[String] = {
      sts match {
        case Array() => xs
        case ys: Array[FileStatus] => traverse(ys.tail, xs ++ trazeStatus(ys.head, onlyFiles))
      }
    }
    traverse(statuses, Array.empty[String])
  }

  def isDirectory(st: FileStatus): Boolean = st.isDirectory
  def getPath(st: FileStatus): String = st.getPath.toString
  def getChild(st: FileStatus): Option[String] = if(isDirectory(st)) Option(getPath(st)) else None
  def status(path: String): Option[Array[FileStatus]] = Try(fs.listStatus(hdfs(path))) match {
    case Success(statuses) => Option(statuses)
    case Failure(exception) =>
      logger.error(exception.getMessage)
      Option.empty[Array[FileStatus]]
  }

}
