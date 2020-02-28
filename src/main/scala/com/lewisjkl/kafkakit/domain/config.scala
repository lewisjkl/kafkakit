package com.lewisjkl.kafkakit.domain

import java.lang.System
import java.nio.file.{Path, Paths}

import cats.data.NonEmptyList
import com.lewisjkl.kafkakit.domain.Config.KafkaCluster
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

final case class Config(
                         kafkaClusters: NonEmptyList[KafkaCluster],
                         defaultClusterNickname: String
                       ) {
  // Option#get is fine here because we will have already validated the default's existence
  def defaultCluster: KafkaCluster =
    kafkaClusters.find(_.nickname == defaultClusterNickname).get
}

object Config {

  implicit val config = Configuration.default

  implicit val decoder: Decoder[Config] = deriveConfiguredDecoder[Config]

  val defaultPath: Path = Paths.get(System.getProperty("user.home")).resolve(".kafkakit.json")

  // TODO - Add smart constructor w/validation for BootstrapServers
  final case class BootstrapServers(value: String) extends AnyVal
  object BootstrapServers {
    implicit val decoder: Decoder[BootstrapServers] = deriveUnwrappedDecoder
  }

  sealed trait EncodingFormat extends Product with Serializable
  object EncodingFormat {
    final case object String extends EncodingFormat
    final case object Avro extends EncodingFormat

    implicit val decoder: Decoder[EncodingFormat] = deriveEnumerationDecoder
  }

  final case class KafkaCluster(
                                 nickname: String,
                                 bootstrapServers: BootstrapServers,
                                 defaultKeyFormat: EncodingFormat,
                                 defaultValueFormat: EncodingFormat,
                                 schemaRegistryUrl: Option[String]
                               )
  object KafkaCluster {
    implicit val decoder: Decoder[KafkaCluster] = deriveConfiguredDecoder
  }

}
