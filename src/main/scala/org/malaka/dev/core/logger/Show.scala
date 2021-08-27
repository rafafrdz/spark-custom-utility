package org.malaka.dev.core.logger

trait Show {
  def info(mssg: String): Unit

  def error(mssg: String): Unit

  def warn(mssg: String): Unit
}
