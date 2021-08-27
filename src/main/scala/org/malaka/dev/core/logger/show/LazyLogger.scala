package org.malaka.dev.core.logger.show

import org.malaka.dev.core.logger.Show
import org.slf4j.{LoggerFactory, Logger => slfLogger}

/**
 * Defines `logger` as a lazy value initialized with an underlying `org.slf4j.Logger`
 * named according to the class into which this trait is mixed.
 */
trait LazyLogger extends Show {
  @transient implicit final lazy val logger: slfLogger = LoggerFactory.getLogger(getClass.getName)
}

object LazyLogger {
  implicit val lazyLoggerDefault: LazyLogger = new LazyLogger {
    override def info(mssg: String): Unit = logger.info(mssg)

    override def error(mssg: String): Unit = logger.error(mssg)

    override def warn(mssg: String): Unit = logger.warn(mssg)
  }
}