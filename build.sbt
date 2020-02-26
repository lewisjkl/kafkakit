inThisBuild(
  List(
    organization := "com.lewisjkl",
    homepage := Some(url("https://github.com/lewisjkl/kafkakit")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "lewisjkl",
        "Jeff Lewis",
        "lewisjkl@me.com",
        url("https://lewisjkl.com")
      )
    )
  )
)

val libraries = List(
  "co.fs2" %% "fs2-io" % "2.2.2",
  "com.github.fd4s" %% "fs2-kafka" % "1.0.0",
  "com.monovore" %% "decline-effect" % "1.0.0",
  "dev.profunktor" %% "console4cats" % "0.8.1",
  "io.circe" %% "circe-fs2" % "0.13.0-M1",
  "io.circe" %% "circe-generic-extras" % "0.13.0",
  "io.confluent" % "kafka-avro-serializer" % "5.4.0",
  "org.typelevel" %% "cats-mtl-core" % "0.7.0"
)

def crossPlugin(x: sbt.librarymanagement.ModuleID) = compilerPlugin(x.cross(CrossVersion.full))

val compilerPlugins = List(
  compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  crossPlugin("org.typelevel" %% "kind-projector" % "0.11.0"),
  crossPlugin("com.github.cb372" % "scala-typed-holes" % "0.1.1")
)

val res = List(
  "confluent" at "https://packages.confluent.io/maven/"
)

val commonSettings = Seq(
  scalaVersion := "2.13.1",
  scalacOptions -= "-Xfatal-warnings",
  scalacOptions ++= Seq(
    "-Ymacro-annotations",
    "-Yimports:" ++ List(
      "scala",
      "scala.Predef",
      "cats",
      "cats.implicits",
      "cats.effect",
      "cats.effect.implicits",
      "cats.effect.concurrent"
    ).mkString(",")
  ),
  name := "kafkakit",
  updateOptions := updateOptions.value.withGigahorse(false),
  resolvers ++= res,
  libraryDependencies ++= libraries ++ compilerPlugins
)

val kafkakit =
  project.in(file(".")).settings(commonSettings).enablePlugins(JavaAppPackaging)
