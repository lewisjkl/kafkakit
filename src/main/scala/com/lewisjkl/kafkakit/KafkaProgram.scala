package com.lewisjkl.kafkakit

import cats.Monad
import cats.effect.{Console, Sync}
import cats.implicits._

final class KafkaProgram[F[_]: Monad: Console] private(kafkaClient: KafkaClient[F]) {
  def listTopics: F[Unit] = kafkaClient
    .listTopics.flatMap(_.toList.traverse(Console[F].putStrLn).as(()))
}

object KafkaProgram {

  def live[F[_]: Console: Sync](kafkaClient: KafkaClient[F]): F[KafkaProgram[F]] = Sync[F].delay {
    new KafkaProgram[F](kafkaClient)
  }

  def apply[F[_]](implicit k: KafkaProgram[F]): KafkaProgram[F] = k
}

