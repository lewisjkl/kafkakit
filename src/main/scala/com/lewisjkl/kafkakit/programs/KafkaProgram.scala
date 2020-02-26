package com.lewisjkl.kafkakit.programs

import com.lewisjkl.kafkakit.algebras.KafkaClient
import com.lewisjkl.kafkakit.algebras.KafkaClient.TopicName

final class KafkaProgram[F[_]: Monad: Console] private(kafkaClient: KafkaClient[F]) {
  def listTopics: F[Unit] = kafkaClient
    .listTopics.flatMap(_.toList.sorted.traverse(Console[F].putStrLn).as(()))

  def consume(topicName: TopicName): fs2.Stream[F, Unit] = {
    kafkaClient.consume(topicName).evalMap(Console[F].putStrLn(_)).handleErrorWith {
      case k: org.apache.kafka.common.KafkaException => fs2.Stream.eval(Console[F].putStrLn(k.getMessage))
    }
  }
}

object KafkaProgram {

  def live[F[_]: Monad: Console](kafkaClient: KafkaClient[F]): KafkaProgram[F] =
    new KafkaProgram[F](kafkaClient)

  def apply[F[_]](implicit k: KafkaProgram[F]): KafkaProgram[F] = k
}

