package com.lewisjkl.kafkakit.algebras

import cats.mtl.ApplicativeAsk
import com.lewisjkl.kafkakit.domain.Config
import com.lewisjkl.kafkakit.domain.Config.AskForConfig
import fs2.kafka._

trait KafkaClient[F[_]] {
  import KafkaClient._
  def listTopics: F[Set[TopicName]]
}

object KafkaClient {

  type TopicName = String

  def live[F[_]: Concurrent: ContextShift: AskForConfig]: KafkaClient[F] =
    new KafkaClient[F] {
      override def listTopics: F[Set[TopicName]] =
        for {
          config <- ApplicativeAsk[F, Config].ask
          names <- adminClientResource(AdminClientSettings[F]
            .withBootstrapServers(config.defaultCluster.bootstrapServers.value)).use { client =>
            client.listTopics.names
          }
        } yield names
    }

}
