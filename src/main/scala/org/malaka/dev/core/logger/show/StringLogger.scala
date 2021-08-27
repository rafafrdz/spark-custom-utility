package org.malaka.dev.core.logger.show

import org.malaka.dev.core.logger.Show

trait StringLogger extends Show {
  @transient implicit final lazy val logger: String => Unit = s => println(s)
}

object StringLogger {
  implicit val stringLoggerDefault: StringLogger = new StringLogger {
    private val ERROR: String = "ERROR"
    private val WARN: String = "WARN"
    private val INFO: String = "INFO"
    private val SEPMSSG: String = ":"

    override def info(mssg: String): Unit = logger(s"$INFO$SEPMSSG $mssg")

    override def error(mssg: String): Unit = logger(s"$ERROR$SEPMSSG $mssg")

    override def warn(mssg: String): Unit = logger(s"$WARN$SEPMSSG $mssg")
  }
}
