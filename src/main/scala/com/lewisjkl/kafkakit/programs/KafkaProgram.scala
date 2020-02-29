package com.lewisjkl.kafkakit.programs

import com.lewisjkl.kafkakit.algebras.KafkaClient
import com.lewisjkl.kafkakit.algebras.KafkaClient.{ConsumerGroup, TopicName}

final class KafkaProgram[F[_]: Monad: Console] private(kafkaClient: KafkaClient[F]) {
  def listTopics: F[Unit] = kafkaClient
    .listTopics.flatMap(_.toList.sorted.traverse(Console[F].putStrLn).as(()))

  def consume(topicName: TopicName, limit: Option[Int], tail: Boolean): fs2.Stream[F, Unit] = {
    val consume_ = kafkaClient.consume(topicName, tail).evalMap(Console[F].putStrLn(_)).handleErrorWith {
      case k: org.apache.kafka.common.KafkaException => fs2.Stream.eval(Console[F].putStrLn(k.getMessage))
    }

    limit match {
      case Some(n) => consume_.take(n.toLong)
      case None => consume_
    }
  }

  def describe(topicName: TopicName): F[Unit] = {
    kafkaClient.describeTopic(topicName).flatMap {
      case Some(desc) => Console[F].putStrLn(desc)
      case None => Console[F].putStrLn(s"$topicName not found.")
    }
  }

  def delete(topicName: TopicName): F[Unit] =
    kafkaClient.deleteTopic(topicName)

  def listConsumerGroups: F[Unit] =
    kafkaClient.listConsumerGroups
      .flatMap(_.toList.sorted.traverse(Console[F].putStrLn).as(()))

  def listConsumerGroupOffsets(consumerGroup: ConsumerGroup): F[Unit] =
    kafkaClient.listConsumerGroupOffsets(consumerGroup)
      .flatMap(_.toList.traverse(o => Console[F].putStrLn(show"${o._1} offset: ${o._2}")).as(()))
}

object KafkaProgram {

  def live[F[_]: Monad: Console](kafkaClient: KafkaClient[F]): KafkaProgram[F] =
    new KafkaProgram[F](kafkaClient)

  def apply[F[_]](implicit k: KafkaProgram[F]): KafkaProgram[F] = k
}

