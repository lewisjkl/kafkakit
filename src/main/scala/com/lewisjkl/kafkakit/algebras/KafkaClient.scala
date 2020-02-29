package com.lewisjkl.kafkakit.algebras

import cats.mtl.MonadState
import com.lewisjkl.kafkakit.domain.Config.{EncodingFormat, KafkaCluster}
import fs2.kafka._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.{Node, TopicPartitionInfo}

trait KafkaClient[F[_]] {
  import KafkaClient._
  def listTopics: F[Set[TopicName]]
  def describeTopic(topicName: TopicName): F[Option[Topic]]
  def consume(topicName: TopicName, tail: Boolean): fs2.Stream[F, KafkaRecord]
  def deleteTopic(topicName: TopicName): F[Unit]
}

object KafkaClient {

  type TopicName = String
  final case class KafkaRecord(key: String, value: String)
  object KafkaRecord {
    implicit val show: Show[KafkaRecord] =
      (t: KafkaRecord) => s"${t.key}\n${t.value}\n"
  }
  final case class KafkaNode(id: String, host: String, port: Int, rack: Option[String])
  object KafkaNode {
    implicit val showOption: Show[Option[String]] = {
      case Some(s) => s" rack: $s"
      case None => ""
    }
    implicit val show: Show[KafkaNode] = (k: KafkaNode) => show"node: ${k.id} at: ${k.host}:${k.port}${k.rack}"
    def create(n: Node): KafkaNode =
      KafkaNode(n.idString, n.host, n.port, Option(n.rack))
  }
  final case class Partition(partition: Int, leader: KafkaNode, replicas: List[KafkaNode], isr: List[KafkaNode])
  object Partition {
    implicit def showKafkaNodes: Show[List[KafkaNode]] = (ks: List[KafkaNode]) => ks.map(_.show).mkString("\n")
    implicit val show: Show[Partition] = (p: Partition) => show"partition: ${p.partition} leaderNode: ${p.leader.id}\nreplicas:\n${p.replicas}"
    import scala.jdk.CollectionConverters._
    def create(t: TopicPartitionInfo): Partition =
      Partition(t.partition, KafkaNode.create(t.leader), t.replicas.asScala.map(KafkaNode.create).toList, t.isr.asScala.map(KafkaNode.create).toList)
  }
  final case class Topic(topicName: TopicName, partitions: List[Partition])
  object Topic {
    implicit val showPartitions: Show[List[Partition]] = (ps: List[Partition]) => ps.map(_.show).mkString("\n\n")
    implicit val show: Show[Topic] = (t: Topic) => show"${t.partitions}"
    import scala.jdk.CollectionConverters._
    def create(t: TopicDescription): Topic = {
      Topic(t.name, t.partitions.asScala.map(Partition.create).toList)
    }
  }

  def live[F[_]: ConcurrentEffect: Timer: ContextShift: MonadState[*[_], KafkaCluster]]: KafkaClient[F] =
    new KafkaClient[F] {

      private def getAdminClientResource =
        for {
          cluster <- Resource.liftF(MonadState[F, KafkaCluster].get)
          res <- adminClientResource(AdminClientSettings[F]
            .withBootstrapServers(cluster.bootstrapServers.value))
        } yield res

      override def listTopics: F[Set[TopicName]] =
        for {
          names <- getAdminClientResource.use { client =>
            client.listTopics.names
          }
        } yield names

      override def describeTopic(topicName: TopicName): F[Option[Topic]] =
        getAdminClientResource.use(_.describeTopics(List(topicName))
          .map(_.get(topicName).map(Topic.create)))

      override def consume(topicName: TopicName, tail: Boolean): fs2.Stream[F, KafkaRecord] = {
        def consumerSettings(cluster: KafkaCluster) = {
          val deserializer = getRecordDeserializer(cluster)
          ConsumerSettings(
            deserializer,
            deserializer
          )
            .withAutoOffsetReset(if (tail) AutoOffsetReset.Latest else AutoOffsetReset.Earliest)
            .withBootstrapServers(cluster.bootstrapServers.value)
            //TODO - allow custom consumer groups
            .withGroupId("group")
        }
        for {
          cluster <- fs2.Stream.eval(MonadState[F, KafkaCluster].get)
          stream <- consumerStream(consumerSettings(cluster))
            .evalTap(_.subscribeTo(topicName))
            .flatMap(_.stream).map(r => KafkaRecord(r.record.key, r.record.value))
        } yield stream
      }

      private def getRecordDeserializer(cluster: KafkaCluster): RecordDeserializer[F, String] = {
        val getDes = getDeserializer(cluster.schemaRegistryUrl) _
        RecordDeserializer.instance[F, String](
          getDes(cluster.defaultKeyFormat),
          getDes(cluster.defaultValueFormat)
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

      override def deleteTopic(topicName: TopicName): F[Unit] =
        getAdminClientResource.use(_.deleteTopic(topicName))
    }

}
