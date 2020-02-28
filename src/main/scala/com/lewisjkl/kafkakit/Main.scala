package com.lewisjkl.kafkakit

import cats.data.NonEmptyList
import com.monovore.decline._
import com.monovore.decline.effect._
import cats.effect.Console.implicits._
import cats.mtl.MonadState
import com.lewisjkl.kafkakit.algebras.KafkaClient
import com.lewisjkl.kafkakit.domain.Config
import com.lewisjkl.kafkakit.domain.Config.KafkaCluster
import com.lewisjkl.kafkakit.programs.{BootstrapProgram, KafkaProgram}
import com.olegpy.meow.effects._

import scala.util.control.NoStackTrace

sealed trait Choice extends Product with Serializable

object Choice {
  final case class ListTopics(altClusterNickname: Option[String]) extends Choice
  final case class ConsumeTopic(
                                 topicName: String,
                                 limit: Option[Int],
                                 fromTail: Boolean,
                                 altClusterNickname: Option[String]) extends Choice
  final case class DeleteTopic(topicName: String, altClusterNickname: Option[String]) extends Choice

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

  val opts: Opts[Choice] =
    NonEmptyList.of[Opts[Choice]](
      Opts.subcommand("topics", "List topics in Kafka")(
        clusterOption.map(ListTopics)
      ),
      Opts.subcommand("consume", "Consume records from a topic") (
        (topicNameArg, limitOption, tailFlag, clusterOption).mapN(ConsumeTopic)
      ),
      Opts.subcommand("delete", "Delete a topic from Kafka")(
        (topicNameArg, clusterOption).mapN(DeleteTopic)
      )
    ).reduceK
}

object Main extends CommandIOApp(
  name = "kafkakit",
  header = "The Kafka CLI You've Always Wanted",
  version = "0.0.1"
) {

  case object KafkaClusterNotFound extends NoStackTrace

  private def runApp[F[_]: Sync: KafkaProgram: MonadState[*[_], KafkaCluster]](config: Config): Choice => F[Unit] = {
    val changeCluster: Option[String] => F[Unit] = _.map { newClusterNickname =>
      val maybeSetNewCluster: F[Unit] = config.kafkaClusters.find(_.nickname === newClusterNickname) match {
        case Some(newCluster) => MonadState[F, KafkaCluster].set(newCluster)
        case None => Sync[F].raiseError(KafkaClusterNotFound)
      }
      maybeSetNewCluster
    }.getOrElse(Sync[F].unit)

    {
      case Choice.ListTopics(altCluster) => changeCluster(altCluster) *> KafkaProgram[F].listTopics
      case Choice.ConsumeTopic(topicName, limit, tail, altCluster) => changeCluster(altCluster) *>
        KafkaProgram[F].consume(topicName, limit, tail).compile.drain
      case Choice.DeleteTopic(topicName, altCluster) => changeCluster(altCluster) *>
        KafkaProgram[F].delete(topicName)
    }
  }

  val makeProgram: Resource[IO, Choice => IO[Unit]] =
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
      makeProgram.use(_.apply(choice))
    }

  override def main: Opts[IO[ExitCode]] = mainOpts.map(_.as(ExitCode.Success))
}
