package com.lewisjkl.kafkakit

import cats.data.NonEmptyList
import com.monovore.decline._
import com.monovore.decline.effect._
import cats.effect.Console.implicits._

sealed trait Choice extends Product with Serializable

object Choice {
  case object Read extends Choice
  case object Write extends Choice
  case object ListTopics extends Choice

  val opts: Opts[Choice] =
    NonEmptyList.of[Opts[Choice]](
      Opts.subcommand("read", "Read data from Kafka")(Opts(Read)),
      Opts.subcommand("write", "Write data to Kafka")(Opts(Write)),
      Opts.subcommand("list", "List topics in Kafka")(Opts(ListTopics))
    ).reduceK
}

object Main extends CommandIOApp(
  name = "kafkakit",
  header = "The Kafka CLI You've Always Wanted",
  version = "0.0.1"
) {

  private def runApp[F[_]: KafkaProgram]: Choice => F[Unit] = {
    case Choice.ListTopics => KafkaProgram[F].listTopics
    case _ => throw new Exception
  }

  val mainOpts: Opts[IO[Unit]] = Choice
    .opts
    .map { choice =>
      (for {
        kafka <- KafkaClient.live[IO]
        program <- KafkaProgram.live[IO](kafka)
      } yield {
        implicit val kafkaProgram: KafkaProgram[IO] = program
        runApp[IO].apply(choice)
      }).flatten
    }

  override def main: Opts[IO[ExitCode]] = mainOpts.map(_.as(ExitCode.Success))
}
