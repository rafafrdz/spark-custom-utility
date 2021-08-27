package org.malaka.dev.io

import org.malaka.dev.core.session.Sparkable
import org.malaka.dev.io.FStatus.{getChild, status, traverseStatuses}

import scala.util.{Failure, Success, Try}

object FSystem extends HadoopFileSystem with Sparkable {


  def rm(path: String, recursive: Boolean = false): Unit = delete(path, recursive)

  def delete(path: String, recursive: Boolean = false): Unit = fs.delete(hdfs(path), recursive)

  def mv(path: String, otherPath: String): Boolean = mover(path, otherPath)

  def mover(path: String, otherPath: String): Boolean =
    Try {
      require(!isDirectory(path), "Path must be a file path")
      lazy val move: Boolean = fs.rename(hdfs(path), hdfs(otherPath))
      delete(otherPath)
      move
      delete(path)
      move
    } match {
      case Success(value) => value
      case Failure(exception) =>
        logger.error(exception.getMessage)
        false
    }

  def mkdirs(path: String, overwrite: Boolean = false): Unit = {
    if (overwrite) delete(path, recursive = true)
    fs.mkdirs(hdfs(path))
  }

  def exist(path: String): Boolean = fs.exists(hdfs(path))

  def isDirectory(path: String): Boolean = fs.isDirectory(hdfs(path))

  def lsTree(root: String, onlyFiles: Boolean = true): Array[String] = {
    status(root) match {
      case Some(statuses) =>
        lazy val paths = traverseStatuses(statuses, onlyFiles)
        if (onlyFiles) paths else root +: paths
      case None => Array.empty[String]
    }
  }

  def lsSubDirectory(root: String, recursive: Boolean = false): Array[String] = {
    if (recursive) lsTree(root, onlyFiles = false).filterNot(_.contains("."))
    else {
      status(root) match {
        case Some(statuses) => statuses.flatMap(st => getChild(st))
        case None => Array.empty[String]
      }
    }
  }

}
