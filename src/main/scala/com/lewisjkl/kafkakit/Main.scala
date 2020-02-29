package com.lewisjkl.kafkakit

import cats.data.NonEmptyList
import com.monovore.decline._
import com.monovore.decline.effect._
import cats.effect.Console.implicits._
import cats.mtl.MonadState
import com.lewisjkl.kafkakit.Choice.AltCluster
import com.lewisjkl.kafkakit.algebras.KafkaClient
import com.lewisjkl.kafkakit.domain.Config
import com.lewisjkl.kafkakit.domain.Config.KafkaCluster
import com.lewisjkl.kafkakit.programs.{BootstrapProgram, KafkaProgram}
import com.olegpy.meow.effects._

import scala.util.control.NoStackTrace

sealed trait Choice extends Product with Serializable

object Choice {
  case object ListTopics extends Choice
  final case class ConsumeTopic(
                                 topicName: String,
                                 limit: Option[Int],
                                 fromTail: Boolean) extends Choice
  final case class DeleteTopic(topicName: String) extends Choice
  final case class DescribeTopic(topicName: String) extends Choice
  case object ListConsumerGroup extends Choice

  final case class AltCluster(nickname: Option[String])

  private val clusterOption = Opts.option[String](
    "cluster",
    "nickname of the cluster to run the given command on",
    "c"
  ).orNone

  private val limitOption = Opts.option[Int](
    "limit",
    "limit the number of results returned",
    "n"
  ).orNone

  private val tailFlag = Opts.flag(
    "tail",
    "consume starting at the tail of the stream",
    "t"
  ).orFalse

  private val topicNameArg = Opts.argument[String](metavar = "topicName")

  private def withAltCluster[A](o: Opts[A]): Opts[(AltCluster, A)] =
    (clusterOption.map(AltCluster), o).tupled

  val opts: Opts[(AltCluster, Choice)] =
    NonEmptyList.of[Opts[(AltCluster, Choice)]](
      Opts.subcommand("topics", "List topics in Kafka")(
        withAltCluster(Opts(ListTopics))
      ),
      Opts.subcommand("consume", "Consume records from a topic") (
        withAltCluster((topicNameArg, limitOption, tailFlag).mapN(ConsumeTopic))
      ),
      Opts.subcommand("delete", "Delete a topic from Kafka")(
        withAltCluster((topicNameArg).map(DeleteTopic))
      ),
      Opts.subcommand("describe", "Describe a topic in Kafka")(
        withAltCluster((topicNameArg).map(DescribeTopic))
      ),
      Opts.subcommand("consumers", "List all consumer groups")(
        withAltCluster(Opts(ListConsumerGroup))
      )
    ).reduceK
}

object Main extends CommandIOApp(
  name = "kafkakit",
  header = "The Kafka CLI You've Always Wanted",
  version = "0.0.2"
) {

  case object KafkaClusterNotFound extends NoStackTrace

  private def runApp[F[_]: Sync: KafkaProgram: MonadState[*[_], KafkaCluster]](config: Config): (AltCluster, Choice) => F[Unit] = {
    val changeCluster: AltCluster => F[Unit] = _.nickname.map { newClusterNickname =>
      val maybeSetNewCluster: F[Unit] = config.kafkaClusters.find(_.nickname === newClusterNickname) match {
        case Some(newCluster) => MonadState[F, KafkaCluster].set(newCluster)
        case None => Sync[F].raiseError(KafkaClusterNotFound)
      }
      maybeSetNewCluster
    }.getOrElse(Sync[F].unit)

    def app(cluster: AltCluster, choice: Choice): F[Unit] = {
      changeCluster(cluster) *>
        (choice match {
        case Choice.ListTopics => KafkaProgram[F].listTopics
        case Choice.ConsumeTopic(topicName, limit, tail) => KafkaProgram[F].consume(topicName, limit, tail).compile.drain
        case Choice.DeleteTopic(topicName) => KafkaProgram[F].delete(topicName)
        case Choice.DescribeTopic(topicName) => KafkaProgram[F].describe(topicName)
        case Choice.ListConsumerGroup => KafkaProgram[F].listConsumerGroups
      })
    }
    app
  }

  val makeProgram: Resource[IO, (AltCluster, Choice) => IO[Unit]] =
    BootstrapProgram.makeConfigLoader[IO].flatMap { configLoader =>
      val kafkaProgram = for {
        config <- configLoader.load
        ref <- Ref[IO].of(config.defaultCluster)
        kafka <- ref.runState { implicit monadState =>
          implicit val kafkaProgram: KafkaProgram[IO] = KafkaProgram.live[IO](KafkaClient.live[IO])
          IO(runApp[IO](config))
        }
      } yield kafka
      Resource.liftF(kafkaProgram)
    }

  val mainOpts: Opts[IO[Unit]] = Choice
    .opts
    .map { choice =>
      makeProgram.use(prog => (prog.apply _).tupled(choice))
    }

  override def main: Opts[IO[ExitCode]] = mainOpts.map(_.as(ExitCode.Success))
}
