package com.lewisjkl.kafkakit.programs

import com.lewisjkl.kafkakit.algebras.ConfigLoader
import com.lewisjkl.kafkakit.domain.Config

object BootstrapProgram {

  def makeConfigLoader[F[_]: Sync: ContextShift]: Resource[F, ConfigLoader[F]] = Blocker[F].evalMap { blocker =>
    ConfigLoader.default(Config.defaultPath, blocker)
  }

}
