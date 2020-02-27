package com.lewisjkl.kafkakit.algebras

import cats.mtl.ApplicativeAsk
import com.lewisjkl.kafkakit.domain.Config
import com.lewisjkl.kafkakit.domain.Config.{AskForConfig, EncodingFormat}
import fs2.kafka._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

trait KafkaClient[F[_]] {
  import KafkaClient._
  def listTopics: F[Set[TopicName]]
  def consume(topicName: TopicName, tail: Boolean): fs2.Stream[F, KafkaRecord]
}

object KafkaClient {

  type TopicName = String
  final case class KafkaRecord(key: String, value: String)
  object KafkaRecord {
    implicit val show: Show[KafkaRecord] =
      (t: KafkaRecord) => s"${t.key}\n${t.value}\n"
  }

  def live[F[_]: ConcurrentEffect: Timer: ContextShift: AskForConfig]: KafkaClient[F] =
    new KafkaClient[F] {
      override def listTopics: F[Set[TopicName]] =
        for {
          config <- ApplicativeAsk[F, Config].ask
          names <- adminClientResource(AdminClientSettings[F]
            .withBootstrapServers(config.defaultCluster.bootstrapServers.value)).use { client =>
            client.listTopics.names
          }
        } yield names

      override def consume(topicName: TopicName, tail: Boolean): fs2.Stream[F, KafkaRecord] = {
        def consumerSettings(config: Config) = {
          val deserializer = getRecordDeserializer(config)
          ConsumerSettings(
            deserializer,
            deserializer
          )
            .withAutoOffsetReset(if (tail) AutoOffsetReset.Latest else AutoOffsetReset.Earliest)
            .withBootstrapServers(config.defaultCluster.bootstrapServers.value)
            //TODO - allow custom consumer groups
            .withGroupId("group")
        }
        for {
          config <- fs2.Stream.eval(ApplicativeAsk[F, Config].ask)
          stream <- consumerStream(consumerSettings(config))
            .evalTap(_.subscribeTo(topicName))
            .flatMap(_.stream).map(r => KafkaRecord(r.record.key, r.record.value))
        } yield stream
      }

      private def getRecordDeserializer(config: Config): RecordDeserializer[F, String] = {
        val getDes = getDeserializer(config.defaultCluster.schemaRegistryUrl) _
        RecordDeserializer.instance[F, String](
          getDes(config.defaultCluster.defaultKeyFormat),
          getDes(config.defaultCluster.defaultValueFormat)
        )
      }

      private def getDeserializer(maybeSchemaRegistryUrl: Option[String])
                                 (encodingFormat: EncodingFormat): F[Deserializer[F, String]] = Sync[F].delay {
        encodingFormat match {
          case EncodingFormat.String => Deserializer[F, String]
          case EncodingFormat.Avro =>
            maybeSchemaRegistryUrl match {
              case Some(schemaRegistryUrl) => Deserializer.delegate[F, String] {
                new KafkaDeserializer[String] {
                  val s = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
                  val kad = new KafkaAvroDeserializer(s)
                    .asInstanceOf[org.apache.kafka.common.serialization.Deserializer[GenericRecord]]
                  def deserialize(topic: String, data: Array[Byte]): String = {
                    val a = kad.deserialize(topic, data)
                    a.toString
                  }
                }
              }.suspend
              case None => Deserializer.failWith("schemaRegistryUrl must be specified in .kafkakit.json if using Avro encodingFormat")
            }
        }
      }
    }

}
