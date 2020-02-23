package com.lewisjkl.kafkakit.programs

import com.lewisjkl.kafkakit.algebras.KafkaClient

final class KafkaProgram[F[_]: Monad: Console] private(kafkaClient: KafkaClient[F]) {
  def listTopics: F[Unit] = kafkaClient
    .listTopics.flatMap(_.toList.sorted.traverse(Console[F].putStrLn).as(()))
}

object KafkaProgram {

  def live[F[_]: Monad: Console](kafkaClient: KafkaClient[F]): KafkaProgram[F] =
    new KafkaProgram[F](kafkaClient)

  def apply[F[_]](implicit k: KafkaProgram[F]): KafkaProgram[F] = k
}

