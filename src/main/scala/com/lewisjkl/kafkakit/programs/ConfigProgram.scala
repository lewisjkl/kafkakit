package com.lewisjkl.kafkakit.programs

import com.lewisjkl.kafkakit.domain.Config

final class ConfigProgram[F[_]: Console] private(config: Config) {

  def outputConfig: F[Unit] = {
    Console[F].putStrLn(config)
  }

}

object ConfigProgram {
  def apply[F[_]](implicit c: ConfigProgram[F]): ConfigProgram[F] = c

  def live[F[_]: Console](config: Config): ConfigProgram[F] =
    new ConfigProgram[F](config)
}
