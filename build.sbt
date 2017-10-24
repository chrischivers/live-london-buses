name := "live-london-buses"

version := "0.1"

scalaVersion := "2.12.3"
val circeVersion = "0.9.0-M1"
val Http4sVersion = "0.18.0-M1"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.9",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.github.mauricio" % "postgresql-async_2.12" % "0.2.21",
  "com.github.etaty" %% "rediscala" % "1.8.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.3",
  "com.xebialabs.restito" % "restito" % "0.9.1" % "test",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.0.7",
  "com.typesafe.akka" % "akka-stream_2.12" % "2.5.4",
  "com.typesafe.akka" % "akka-actor_2.12" % "2.5.4",
  "com.typesafe.akka" % "akka-http_2.12" % "10.0.10",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4" % Test,
  "com.github.davidb" % "metrics-influxdb" % "0.9.3",
  "nl.grons" %% "metrics-scala" % "3.5.9_a2.4",
  "com.internetitem" % "logback-elasticsearch-appender" % "1.6",
  "org.typelevel" %% "cats-core" % "1.0.0-MF",
  "com.github.cb372" % "scalacache-core_2.12" % "0.10.0",
  "com.github.cb372" %% "scalacache-guava" % "0.10.0"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" ,
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "org.http4s"     %% "http4s-blaze-server",
  "org.http4s"     %% "http4s-circe",
  "org.http4s"     %% "http4s-twirl",
  "org.http4s"     %% "http4s-dsl",
  "org.http4s"     %% "http4s-blaze-client"
).map(_ % Http4sVersion)

lazy val root = (project in file(".")).enablePlugins(SbtTwirl)

parallelExecution in Test := false