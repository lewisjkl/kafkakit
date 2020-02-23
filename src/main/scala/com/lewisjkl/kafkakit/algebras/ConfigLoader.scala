package com.lewisjkl.kafkakit.algebras

import java.nio.file.Path

import cats.mtl.{ApplicativeAsk, DefaultApplicativeAsk}
import com.lewisjkl.kafkakit.domain.Config

trait ConfigLoader[F[_]] {
  def load: F[Config]
}

object ConfigLoader extends LowPriority {

  def apply[F[_]](implicit c: ConfigLoader[F]): ConfigLoader[F] = c

  def default[F[_]: Sync: ContextShift](configPath: Path, blocker: Blocker): F[ConfigLoader[F]] = Sync[F].delay {
    new ConfigLoader[F] {
      override def load: F[Config] =
        fs2.io.file
          .readAll(configPath, blocker, 4096)
          .through(io.circe.fs2.byteStreamParser)
          .through(io.circe.fs2.decoder[F, Config])
          .compile
          .lastOrError
    }
  }

}

trait LowPriority {

  implicit def deriveAskFromLoader[F[_]: ConfigLoader: Applicative]: ApplicativeAsk[F, Config] =
    new DefaultApplicativeAsk[F, Config] {
      val applicative: Applicative[F] = Applicative[F]
      val ask: F[Config] = ConfigLoader[F].load
    }

}
