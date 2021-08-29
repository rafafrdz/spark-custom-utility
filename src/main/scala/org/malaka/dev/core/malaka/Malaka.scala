package org.malaka.dev.core.malaka

import scala.language.higherKinds

trait Malaka[F[_], Data] {
  protected def data: F[Data]
}

object Malaka {

//  def build[Data, F <: Malaka[Data]](implicit build: BuilderMalaka[Data, F]): F = build.mk // todo. revisar esto
}