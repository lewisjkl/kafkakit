package com.lewisjkl.kafkakit

import cats.effect._
import fs2.kafka._

trait KafkaClient[F[_]] {
  import KafkaClient._
  def listTopics: F[Set[TopicName]]
}

object KafkaClient {

  type TopicName = String

  final case class BootstrapServers(value: String) extends AnyVal

  def live[F[_]: Concurrent: ContextShift]: F[KafkaClient[F]] = Sync[F].delay {
    new KafkaClient[F] {
      override def listTopics: F[Set[TopicName]] = adminClientResource(AdminClientSettings[F]
        .withBootstrapServers("kafka1-broker-staging.vnerd.com:9092")).use { client =>
        client.listTopics.names
      }
    }
  }

}
