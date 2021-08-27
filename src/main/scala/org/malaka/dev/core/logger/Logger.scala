package org.malaka.dev.core.logger

import org.malaka.dev.core.logger.show.{LazyLogger, StringLogger}

trait Logger[S <: Show] {
  val show: S

  def info(mssg: String): Unit = show.info(mssg)

  def error(mssg: String): Unit = show.error(mssg)

  def warn(mssg: String): Unit = show.warn(mssg)
}

object Logger {
  def build[Shw <: Show](implicit s: Shw): Logger[Shw] = new Logger[Shw] {
    val show: Shw = s
  }

  implicit lazy val lazyLoggerDefault: Logger[LazyLogger] = Logger.build(LazyLogger.lazyLoggerDefault)
  implicit lazy val stringLoggerDefault: Logger[StringLogger] = Logger.build(StringLogger.stringLoggerDefault)
}
