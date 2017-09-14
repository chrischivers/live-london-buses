name := "live-london-buses"

version := "0.1"

scalaVersion := "2.12.3"
val circeVersion = "0.8.0"
val Http4sVersion = "0.17.0-M3"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.github.mauricio" % "postgresql-async_2.12" % "0.2.21"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.4"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"
libraryDependencies += "com.xebialabs.restito" % "restito" % "0.9.1" % "test"
libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.0.7"
libraryDependencies += "com.github.cb372" % "scalacache-core_2.12" % "0.9.4"
libraryDependencies += "com.github.cb372" %% "scalacache-guava" % "0.9.4"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "org.http4s"     %% "http4s-blaze-server" % Http4sVersion,
  "org.http4s"     %% "http4s-circe"        % Http4sVersion,
  "org.http4s"     %% "http4s-twirl"        % Http4sVersion,
  "org.http4s"     %% "http4s-dsl"          % Http4sVersion,
  "org.http4s"     %% "http4s-blaze-client" % Http4sVersion
)

lazy val root = (project in file(".")).enablePlugins(SbtTwirl)